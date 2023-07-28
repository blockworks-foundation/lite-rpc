use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use tracing::{debug_span, instrument, Instrument, span};
use anyhow::{anyhow, bail, Context, Error};
use dashmap::DashMap;
use fan::tokio::mpsc::FanOut;
use futures::sink::Fanout;
use itertools::{any, Itertools};
use log::{debug, error, info, trace, warn};
use quinn::{Connecting, Connection, ConnectionError, Endpoint, SendStream, ServerConfig, TransportConfig, VarInt};
use rcgen::generate_simple_self_signed;
use rustls::{Certificate, PrivateKey};
use rustls::server::ResolvesServerCert;
use serde::{Deserialize, Serialize};
use solana_sdk::packet::PACKET_DATA_SIZE;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::quic::QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::VersionedTransaction;
use tokio::net::ToSocketAddrs;
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{RwLock, RwLockReadGuard};
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use tracing::field::debug;
use crate::proxy_request_format::TpuForwardingRequest;
use crate::quic_connection_utils::{connection_stats, QuicConnectionUtils};
use crate::quinn_auto_reconnect::AutoReconnect;
use crate::tpu_quic_client::{send_txs_to_tpu_static, SingleTPUConnectionManager, TpuQuicClient};
use crate::tls_config_provicer::{ProxyTlsConfigProvider, SelfSignedTlsConfigProvider};
use crate::util::AnyhowJoinHandle;

// TODO tweak this value - solana server sets 256
// setting this to "1" did not make a difference!
const MAX_CONCURRENT_UNI_STREAMS: u32 = 24;

pub struct QuicForwardProxy {
    endpoint: Endpoint,
    validator_identity: Arc<Keypair>,
    tpu_quic_client: TpuQuicClient,
}

/// internal structure with transactions and target TPU
#[derive(Debug)]
struct ForwardPacket {
    pub transactions: Vec<VersionedTransaction>,
    pub tpu_address: SocketAddr,
}

impl QuicForwardProxy {
    pub async fn new(
        proxy_listener_addr: SocketAddr,
        tls_config: &SelfSignedTlsConfigProvider,
        validator_identity: Arc<Keypair>) -> anyhow::Result<Self> {
        let server_tls_config = tls_config.get_server_tls_crypto_config();
        let mut quinn_server_config = ServerConfig::with_crypto(Arc::new(server_tls_config));

        // note: this config must be aligned with lite-rpc's client config
        let transport_config = Arc::get_mut(&mut quinn_server_config.transport).unwrap();
        // TODO experiment with this value
        transport_config.max_concurrent_uni_streams(VarInt::from_u32(MAX_CONCURRENT_UNI_STREAMS));
        // no bidi streams used
        transport_config.max_concurrent_bidi_streams(VarInt::from_u32(0));
        let timeout = Duration::from_secs(10).try_into().unwrap();
        transport_config.max_idle_timeout(Some(timeout));
        transport_config.keep_alive_interval(Some(Duration::from_millis(500)));
        transport_config.stream_receive_window((PACKET_DATA_SIZE as u32).into());
        transport_config.receive_window((PACKET_DATA_SIZE as u32 * MAX_CONCURRENT_UNI_STREAMS).into());

        let endpoint = Endpoint::server(quinn_server_config, proxy_listener_addr).unwrap();
        info!("Quic proxy uses validator identity {}", validator_identity.pubkey());

        let tpu_quic_client =
            TpuQuicClient::new_with_validator_identity(validator_identity.as_ref()).await;

        Ok(Self { endpoint, validator_identity, tpu_quic_client })

    }

    pub async fn start_services(
        mut self,
    ) -> anyhow::Result<()> {
        let exit_signal = Arc::new(AtomicBool::new(false));

        let tpu_quic_client_copy = self.tpu_quic_client.clone();
        let endpoint = self.endpoint.clone();
        let (forwarder_channel, forward_receiver) = tokio::sync::mpsc::channel(1000);

        let quic_proxy: AnyhowJoinHandle = tokio::spawn(self.listen(exit_signal.clone(), endpoint, forwarder_channel));

        let forwarder: AnyhowJoinHandle = tokio::spawn(tx_forwarder(tpu_quic_client_copy, forward_receiver, exit_signal.clone()));

        tokio::select! {
            res = quic_proxy => {
                bail!("TPU Quic Proxy server exited unexpectedly {res:?}");
            },
            res = forwarder => {
                bail!("TPU Quic Tx forwarder exited unexpectedly {res:?}");
            },
        }
    }

    async fn listen(self, exit_signal: Arc<AtomicBool>, endpoint: Endpoint, forwarder_channel: Sender<ForwardPacket>) -> anyhow::Result<()> {
        info!("TPU Quic Proxy server listening on {}", endpoint.local_addr()?);

        while let Some(connecting) = endpoint.accept().await {
            let exit_signal = exit_signal.clone();
            let validator_identity_copy = self.validator_identity.clone();
            let tpu_quic_client = self.tpu_quic_client.clone();
            let forwarder_channel_copy = forwarder_channel.clone();
            tokio::spawn(async move {
                let connection = connecting.await.context("handshake").unwrap();
                match accept_client_connection(connection, forwarder_channel_copy,
                                               tpu_quic_client, exit_signal, validator_identity_copy)
                    .await {
                    Ok(()) => {}
                    Err(err) => {
                        error!("setup connection failed: {reason}", reason = err);
                    }
                }
            });
        }

        bail!("TPU Quic Proxy server stopped");
    }
}


// TODO use interface abstraction for connection_per_tpunode
#[tracing::instrument(skip_all, level = "debug")]
async fn accept_client_connection(client_connection: Connection, forwarder_channel: Sender<ForwardPacket>,
                                  tpu_quic_client: TpuQuicClient,
                                  exit_signal: Arc<AtomicBool>, validator_identity: Arc<Keypair>) -> anyhow::Result<()> {
    debug!("inbound connection established, client {}", client_connection.remote_address());

    // let active_tpu_connection =
    //     TpuQuicClient::new_with_validator_identity(validator_identity.as_ref()).await;

    loop {
        let maybe_stream = client_connection.accept_uni().await;
        let result = match maybe_stream {
            Err(quinn::ConnectionError::ApplicationClosed(reason)) => {
                debug!("connection closed by client - reason: {:?}", reason);
                if reason.error_code != VarInt::from_u32(0) {
                    return Err(anyhow!("connection closed by client with unexpected reason: {:?}", reason));
                }
                debug!("connection gracefully closed by client");
                return Ok(());
            },
            Err(e) => {
                error!("failed to accept stream: {}", e);
                return Err(anyhow::Error::msg("error accepting stream"));
            }
            Ok(recv_stream) => {
                let exit_signal_copy = exit_signal.clone();
                let validator_identity_copy = validator_identity.clone();
                let tpu_quic_client_copy = tpu_quic_client.clone();

                let forwarder_channel_copy = forwarder_channel.clone();
                tokio::spawn(async move {

                    let raw_request = recv_stream.read_to_end(10_000_000).await // TODO extract to const
                        .unwrap();
                    trace!("read proxy_request {} bytes", raw_request.len());

                    let proxy_request = TpuForwardingRequest::deserialize_from_raw_request(&raw_request);

                    trace!("proxy request details: {}", proxy_request);
                    let tpu_identity = proxy_request.get_identity_tpunode();
                    let tpu_address = proxy_request.get_tpu_socket_addr();
                    let txs = proxy_request.get_transactions();

                    debug!("enqueue transaction batch of size {} to address {}", txs.len(), tpu_address);
                    // tpu_quic_client_copy.send_txs_to_tpu(tpu_address, &txs, exit_signal_copy).await;
                    forwarder_channel_copy.send(ForwardPacket { transactions: txs, tpu_address }).await.unwrap();

                    // debug!("connection stats (proxy inbound): {}", connection_stats(&client_connection));

                });

                Ok(())
            },
        }; // -- result

        if let Err(e) = result {
            return Err(e);
        }

    } // -- loop
}

// takes transactions from upstream clients and forwards them to the TPU
async fn tx_forwarder(tpu_quic_client: TpuQuicClient, mut transaction_channel: Receiver<ForwardPacket>, exit_signal: Arc<AtomicBool>) -> anyhow::Result<()> {
    info!("TPU Quic forwarder started");

    let mut agents: HashMap<SocketAddr, FanOut<ForwardPacket>> = HashMap::new();

    let tpu_quic_client_copy = tpu_quic_client.clone();
    loop {
        // TODO add exit

        let forward_packet = transaction_channel.recv().await.expect("channel closed unexpectedly");
        let tpu_address = forward_packet.tpu_address;

        if !agents.contains_key(&tpu_address) {
            // TODO cleanup agent after a while of iactivity

            let mut senders = Vec::new();
            for i in 0..4 {
                let (sender, mut receiver) = channel::<ForwardPacket>(100000);
                senders.push(sender);
                let endpoint = tpu_quic_client.get_endpoint().clone();
                let exit_signal = exit_signal.clone();
                tokio::spawn(async move {
                    debug!("Start Quic forwarder agent for TPU {}", tpu_address);
                    // TODO pass+check the tpu_address
                    // TODO connect
                    // TODO consume queue
                    // TODO exit signal

                    let auto_connection = AutoReconnect::new(endpoint, tpu_address);
                    // let mut connection = tpu_quic_client_copy.create_connection(tpu_address).await.expect("handshake");
                    loop {

                        let exit_signal = exit_signal.clone();
                        loop {
                            let packet = receiver.recv().await.unwrap();
                            assert_eq!(packet.tpu_address, tpu_address, "routing error");

                            let mut transactions_batch = packet.transactions;

                            let mut batch_size = 1;
                            while let Ok(more) = receiver.try_recv() {
                                transactions_batch.extend(more.transactions);
                                batch_size += 1;
                            }
                            if batch_size > 1 {
                                debug!("encountered batch of size {}", batch_size);
                            }

                            debug!("forwarding transaction batch of size {} to address {}", transactions_batch.len(), packet.tpu_address);

                            // TODo move send_txs_to_tpu_static to tpu_quic_client
                            let result = timeout(Duration::from_millis(500),
                                                 send_txs_to_tpu_static(&auto_connection, &transactions_batch)).await;
                            // .expect("timeout sending data to TPU node");

                            if result.is_err() {
                                warn!("send_txs_to_tpu_static result {:?} - loop over errors", result);
                            } else {
                                debug!("send_txs_to_tpu_static sent {}", transactions_batch.len());
                            }

                        }

                    }

                });

            }

            let fanout = FanOut::new(senders);

            agents.insert(tpu_address, fanout);

        } // -- new agent

        let agent_channel = agents.get(&tpu_address).unwrap();

        agent_channel.send(forward_packet).await.unwrap();

        let mut batch_size = 1;
        while let Ok(more) = transaction_channel.try_recv() {
            agent_channel.send(more).await.unwrap();
            batch_size += 1;
        }
        if batch_size > 1 {
            debug!("encountered batch of size {}", batch_size);
        }


        // check if the tpu has already a task+queue running, if not start one, sort+queue packets by tpu address
        // maintain the health of a TPU connection, debounce errors; if failing, drop the respective messages

        // let exit_signal_copy = exit_signal.clone();
        // debug!("send transaction batch of size {} to address {}", forward_packet.transactions.len(), forward_packet.tpu_address);
        // // TODO: this will block/timeout if the TPU is not available
        // timeout(Duration::from_millis(500),
        //         tpu_quic_client_copy.send_txs_to_tpu(tpu_address, &forward_packet.transactions, exit_signal_copy)).await;
        // tpu_quic_client_copy.send_txs_to_tpu(forward_packet.tpu_address, &forward_packet.transactions, exit_signal_copy).await;

    }

    bail!("TPU Quic forward service stopped");
}


