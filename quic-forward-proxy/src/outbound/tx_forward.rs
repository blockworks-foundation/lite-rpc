use crate::quic_util::SkipServerVerification;
use crate::quinn_auto_reconnect::AutoReconnect;
use crate::shared::ForwardPacket;
use crate::util::timeout_fallback;
use crate::validator_identity::ValidatorIdentity;
use anyhow::{bail, Context};
use futures::future::join_all;
use log::{debug, info, warn};
use quinn::{
    ClientConfig, Endpoint, EndpointConfig, IdleTimeout, TokioRuntime, TransportConfig, VarInt,
};
use solana_sdk::quic::QUIC_MAX_TIMEOUT_MS;
use solana_sdk::transaction::VersionedTransaction;
use solana_streamer::nonblocking::quic::ALPN_TPU_PROTOCOL_ID;
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;

const MAX_PARALLEL_STREAMS: usize = 6;
pub const PARALLEL_TPU_CONNECTION_COUNT: usize = 4;

// takes transactions from upstream clients and forwards them to the TPU
pub async fn tx_forwarder(
    validator_identity: ValidatorIdentity,
    mut transaction_channel: Receiver<ForwardPacket>,
    exit_signal: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    info!("TPU Quic forwarder started");

    let endpoint = new_endpoint_with_validator_identity(validator_identity).await;

    let (broadcast_in, _) = tokio::sync::broadcast::channel::<Arc<ForwardPacket>>(1000);

    let mut agents: HashMap<SocketAddr, Vec<Arc<AtomicBool>>> = HashMap::new();

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
            let mut agent_exit_signals = Vec::new();
            for connection_idx in 1..PARALLEL_TPU_CONNECTION_COUNT {
                let global_exit_signal = exit_signal.clone();
                let agent_exit_signal = Arc::new(AtomicBool::new(false));
                let endpoint_copy = endpoint.clone();
                let agent_exit_signal_copy = agent_exit_signal.clone();
                // by subscribing we expect to get a copy of each packet
                let mut per_connection_receiver = broadcast_in.subscribe();
                tokio::spawn(async move {
                    debug!(
                        "Start Quic forwarder agent #{} for TPU {}",
                        connection_idx, tpu_address
                    );
                    if global_exit_signal.load(Ordering::Relaxed) {
                        warn!("Caught global exit signal - stopping agent thread");
                        return;
                    }
                    if agent_exit_signal_copy.load(Ordering::Relaxed) {
                        warn!("Caught exit signal for this agent - stopping agent thread");
                        return;
                    }

                    // get a copy of the packet from broadcast channel
                    let auto_connection = AutoReconnect::new(endpoint_copy, tpu_address);

                    // TODO check exit signal (using select! or maybe replace with oneshot)
                    let _exit_signal_copy = global_exit_signal.clone();
                    while let Ok(packet) = per_connection_receiver.recv().await {
                        if packet.tpu_address != tpu_address {
                            continue;
                        }

                        let mut transactions_batch: Vec<VersionedTransaction> =
                            packet.transactions.clone();

                        while let Ok(more) = per_connection_receiver.try_recv() {
                            if more.tpu_address != tpu_address {
                                continue;
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

                        if result.is_err() {
                            warn!(
                                "got send_txs_to_tpu_static error {:?} - loop over errors",
                                result
                            );
                        } else {
                            debug!("send_txs_to_tpu_static sent {}", transactions_batch.len());
                            debug!(
                                "Outbound connection stats: {}",
                                &auto_connection.connection_stats().await
                            );
                        }
                    } // -- while all packtes from channel

                    warn!(
                        "Quic forwarder agent #{} for TPU {} exited",
                        connection_idx, tpu_address
                    );
                }); // -- spawned thread for one connection to one TPU
                agent_exit_signals.push(agent_exit_signal);
            } // -- for parallel connections to one TPU

            // FanOut::new(senders)
            agent_exit_signals
        }); // -- new agent

        let _agent_channel = agents.get(&tpu_address).unwrap();

        if broadcast_in.len() > 5 {
            debug!("tx-forward queue len: {}", broadcast_in.len())
        }

        // TODO use agent_exit signal to clean them up
        broadcast_in
            .send(forward_packet)
            .expect("send must succeed");
    } // -- loop over transactions from upstream channels

    // not reachable
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
    let timeout = IdleTimeout::try_from(Duration::from_millis(QUIC_MAX_TIMEOUT_MS as u64)).unwrap();
    transport_config.max_idle_timeout(Some(timeout));
    transport_config.keep_alive_interval(None);

    config.transport_config(Arc::new(transport_config));

    endpoint.set_default_client_config(config);

    endpoint
}

// send potentially large amount of transactions to a single TPU
#[tracing::instrument(skip_all, level = "debug")]
async fn send_tx_batch_to_tpu(auto_connection: &AutoReconnect, txs: &[VersionedTransaction]) {
    for chunk in txs.chunks(MAX_PARALLEL_STREAMS) {
        let all_send_fns = chunk
            .iter()
            .map(|tx| bincode::serialize(tx).unwrap())
            .map(|tx_raw| {
                auto_connection.send_uni(tx_raw) // ignores error
            });

        join_all(all_send_fns).await;
    }
}
