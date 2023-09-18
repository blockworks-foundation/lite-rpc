use crate::outbound::debouncer::Debouncer;
use crate::outbound::sharder::Sharder;
use crate::quic_util::SkipServerVerification;
use crate::quinn_auto_reconnect::AutoReconnect;
use crate::shared::ForwardPacket;
use crate::util::timeout_fallback;
use crate::validator_identity::ValidatorIdentity;
use anyhow::{bail, Context};
use futures::future::join_all;
use log::{debug, info, trace, warn};
use quinn::{
    ClientConfig, Endpoint, EndpointConfig, IdleTimeout, TokioRuntime, TransportConfig, VarInt,
};
use solana_sdk::quic::QUIC_MAX_TIMEOUT;
use solana_streamer::nonblocking::quic::ALPN_TPU_PROTOCOL_ID;
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;

const MAX_PARALLEL_STREAMS: usize = 6;
pub const PARALLEL_TPU_CONNECTION_COUNT: usize = 4;
const AGENT_SHUTDOWN_IDLE: Duration = Duration::from_millis(2500); // ms; should be 4x400ms+buffer

struct AgentHandle {
    pub tpu_address: SocketAddr,
    pub agent_exit_signal: Arc<AtomicBool>,
    pub last_used_at: Arc<RwLock<Instant>>,
}

impl AgentHandle {
    pub async fn touch(&self) {
        let mut timestamp = self.last_used_at.write().await;
        *timestamp = Instant::now();
    }
}

// takes transactions from upstream clients and forwards them to the TPU
pub async fn tx_forwarder(
    validator_identity: ValidatorIdentity,
    mut transaction_channel: Receiver<ForwardPacket>,
    exit_signal: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    info!("TPU Quic forwarder started");

    let endpoint = new_endpoint_with_validator_identity(validator_identity).await;

    let (broadcast_in, _) = tokio::sync::broadcast::channel::<Arc<ForwardPacket>>(1024);

    let mut agents: HashMap<SocketAddr, AgentHandle> = HashMap::new();
    let agent_shutdown_debouncer = Debouncer::new(Duration::from_millis(200));

    loop {
        if exit_signal.load(Ordering::Relaxed) {
            bail!("exit signal received");
        }

        let forward_packet = Arc::new(
            transaction_channel
                .recv()
                .await
                .expect("channel closed unexpectedly"),
        );
        let tpu_address = forward_packet.tpu_address;

        agents.entry(tpu_address).or_insert_with(|| {
            let agent_exit_signal = Arc::new(AtomicBool::new(false));

            for connection_idx in 0..PARALLEL_TPU_CONNECTION_COUNT {
                let sharder =
                    Sharder::new(connection_idx as u32, PARALLEL_TPU_CONNECTION_COUNT as u32);
                let global_exit_signal = exit_signal.clone();
                let endpoint_copy = endpoint.clone();
                let agent_exit_signal_copy = agent_exit_signal.clone();
                let mut per_connection_receiver = broadcast_in.subscribe();
                tokio::spawn(async move {
                    debug!(
                        "Start Quic forwarder agent #{} for TPU {}",
                        connection_idx, tpu_address
                    );
                    // get a copy of the packet from broadcast channel
                    let auto_connection = AutoReconnect::new(endpoint_copy, tpu_address);

                    // TODO check exit signal (using select! or maybe replace with oneshot)
                    let _exit_signal_copy = global_exit_signal.clone();
                    'tx_channel_loop: loop {
                        let timeout_result = timeout_fallback(per_connection_receiver.recv()).await;

                        let maybe_packet = match timeout_result {
                            Ok(recv) => recv,
                            Err(_elapsed) => continue 'tx_channel_loop,
                        };

                        if global_exit_signal.load(Ordering::Relaxed) {
                            warn!("Caught global exit signal, {} remaining - stopping agent thread",
                                per_connection_receiver.len());
                            break 'tx_channel_loop;
                        }
                        if agent_exit_signal_copy.load(Ordering::Relaxed) {
                            if per_connection_receiver.is_empty() {
                                debug!("Caught exit signal for this agent ({} #{}) - stopping agent thread",
                                    tpu_address, connection_idx);
                                break 'tx_channel_loop;
                            } else if auto_connection.is_permanent_dead().await {
                                info!("Caught exit signal for this agent ({} #{}), {} remaining but connection is dead - stopping",
                                tpu_address, connection_idx,
                                per_connection_receiver.len());
                                break 'tx_channel_loop;
                            } else {
                                trace!("Caught exit signal for this agent ({} #{}), {} remaining - continue",
                                tpu_address, connection_idx,
                                per_connection_receiver.len());
                            }

                        }

                        if let Err(_recv_error) = maybe_packet {
                            break 'tx_channel_loop;
                        }

                        let packet = maybe_packet.unwrap();

                        if packet.tpu_address != tpu_address {
                            continue 'tx_channel_loop;
                        }
                        if !sharder.matching(packet.shard_hash) {
                            continue 'tx_channel_loop;
                        }

                        if auto_connection.is_permanent_dead().await {
                            warn!("Agent ({} #{}) connection permanently dead, {} remaining - stopping",
                                tpu_address, connection_idx,
                                per_connection_receiver.len());
                            break 'tx_channel_loop;
                        }

                        let mut transactions_batch: Vec<Vec<u8>> = packet.transactions.clone();

                        'more: while let Ok(more) = per_connection_receiver.try_recv() {
                            if more.tpu_address != tpu_address {
                                continue 'more;
                            }
                            if !sharder.matching(more.shard_hash) {
                                continue 'more;
                            }
                            transactions_batch.extend(more.transactions.clone());
                        }

                        debug!(
                            "forwarding transaction batch of size {} to address {}",
                            transactions_batch.len(),
                            packet.tpu_address
                        );

                        let result = timeout_fallback(send_tx_batch_to_tpu(
                            &auto_connection,
                            &transactions_batch,
                        ))
                        .await
                        .context(format!(
                            "send txs to tpu node {}",
                            auto_connection.target_address
                        ));

                        match result {
                            Ok(()) => {
                                debug!("send_txs_to_tpu_static sent {}", transactions_batch.len());
                                debug!(
                                    "Outbound connection stats: {}",
                                    &auto_connection.connection_stats().await
                                );
                            }
                            Err(err) => {
                                warn!("got send_txs_to_tpu_static error {} - loop over errors", err);
                            }
                        }
                    } // -- while all packtes from channel


                    auto_connection.force_shutdown().await;
                    warn!(
                        "Quic forwarder agent #{} for TPU {} exited; shut down connection",
                        connection_idx, tpu_address
                    );
                }); // -- spawned thread for one connection to one TPU
            } // -- for parallel connections to one TPU

            let now = Instant::now();
            AgentHandle {
                tpu_address,
                agent_exit_signal,
                last_used_at: Arc::new(RwLock::new(now))
            }
        }); // -- new agent

        let agent = agents.get(&tpu_address).unwrap();
        agent.touch().await;

        if agent_shutdown_debouncer.can_fire() {
            cleanup_agents(&mut agents, &tpu_address).await;
        }

        if broadcast_in.len() > 5 {
            debug!("tx-forward queue len: {}", broadcast_in.len())
        }

        broadcast_in
            .send(forward_packet)
            .expect("send must succeed");
    } // -- loop over transactions from upstream channels

    // not reachable
}

async fn cleanup_agents(
    agents: &mut HashMap<SocketAddr, AgentHandle>,
    current_tpu_address: &SocketAddr,
) {
    let now = Instant::now();
    let mut to_shutdown = Vec::new();
    for (tpu_address, handle) in &*agents {
        if tpu_address == current_tpu_address {
            continue;
        }

        let unused_period = {
            let last_used_at = handle.last_used_at.read().await;
            now - *last_used_at
        };

        if unused_period > AGENT_SHUTDOWN_IDLE {
            to_shutdown.push(tpu_address.to_owned())
        }
    }

    for tpu_address in to_shutdown.iter() {
        if let Some(removed_agent) = agents.remove(tpu_address) {
            let was_signaled = removed_agent
                .agent_exit_signal
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok();
            if was_signaled {
                let unused_period = {
                    let last_used_ts = removed_agent.last_used_at.read().await;
                    Instant::now() - *last_used_ts
                };
                debug!(
                    "Idle Agent for tpu node {} idle for {}ms - sending exit signal",
                    removed_agent.tpu_address,
                    unused_period.as_millis()
                );
            }
        }
    }
}

/// takes a validator identity and creates a new QUIC client; appears as staked peer to TPU
// note: ATM the provided identity might or might not be a valid validator keypair
async fn new_endpoint_with_validator_identity(validator_identity: ValidatorIdentity) -> Endpoint {
    info!(
        "Setup TPU Quic stable connection with validator identity {} ...",
        validator_identity
    );
    // the counterpart of this function is get_remote_pubkey+get_pubkey_from_tls_certificate
    let (certificate, key) = new_self_signed_tls_certificate(
        &validator_identity.get_keypair_for_tls(),
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
    )
    .expect("Failed to initialize QUIC connection certificates");

    create_tpu_client_endpoint(certificate, key)
}

fn create_tpu_client_endpoint(
    certificate: rustls::Certificate,
    key: rustls::PrivateKey,
) -> Endpoint {
    let mut endpoint = {
        let client_socket =
            solana_net_utils::bind_in_range(IpAddr::V4(Ipv4Addr::UNSPECIFIED), (8000, 10000))
                .expect("create_endpoint bind_in_range")
                .1;
        let config = EndpointConfig::default();
        quinn::Endpoint::new(config, None, client_socket, TokioRuntime)
            .expect("create_endpoint quinn::Endpoint::new")
    };

    let mut crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_single_cert(vec![certificate], key)
        .expect("Failed to set QUIC client certificates");

    crypto.enable_early_data = true;

    crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

    let mut config = ClientConfig::new(Arc::new(crypto));

    // note: this should be aligned with solana quic server's endpoint config
    let mut transport_config = TransportConfig::default();
    // no remotely-initiated streams required
    transport_config.max_concurrent_uni_streams(VarInt::from_u32(0));
    transport_config.max_concurrent_bidi_streams(VarInt::from_u32(0));
    let timeout = IdleTimeout::try_from(QUIC_MAX_TIMEOUT).unwrap();
    transport_config.max_idle_timeout(Some(timeout));
    transport_config.keep_alive_interval(Some(Duration::from_millis(500)));
    transport_config.enable_segmentation_offload(false);

    config.transport_config(Arc::new(transport_config));

    endpoint.set_default_client_config(config);

    endpoint
}

// send potentially large amount of transactions to a single TPU
#[tracing::instrument(skip_all, level = "debug")]
async fn send_tx_batch_to_tpu(auto_connection: &AutoReconnect, txs: &[Vec<u8>]) {
    for chunk in txs.chunks(MAX_PARALLEL_STREAMS) {
        let all_send_fns = chunk.iter().map(|tx_raw| {
            auto_connection.send_uni(tx_raw) // ignores error
        });

        join_all(all_send_fns).await;
    }
}
