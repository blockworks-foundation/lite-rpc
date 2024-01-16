use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use anyhow::bail;
use solana_lite_rpc_core::structures::transaction_sent_info::SentTransactionInfo;
use std::time::Duration;

use itertools::Itertools;
use log::{debug, info, trace, warn};
use quinn::{ClientConfig, Endpoint, EndpointConfig, TokioRuntime, TransportConfig, VarInt};
use solana_sdk::pubkey::Pubkey;

use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::{broadcast::Receiver, RwLock};

use crate::quic_connection_utils::{QuicConnectionParameters, SkipServerVerification};
use solana_lite_rpc_core::network_utils::apply_gso_workaround;
use solana_lite_rpc_core::structures::proxy_request_format::{TpuForwardingRequest, TxData};

use crate::tpu_utils::quinn_auto_reconnect::AutoReconnect;

#[derive(Clone, Copy, Debug)]
pub struct TpuNode {
    pub tpu_identity: Pubkey,
    pub tpu_address: SocketAddr,
}

pub struct QuicProxyConnectionManager {
    endpoint: Endpoint,
    simple_thread_started: AtomicBool,
    proxy_addr: SocketAddr,
    current_tpu_nodes: Arc<RwLock<Vec<TpuNode>>>,
    exit_signal: Arc<AtomicBool>,
}

const CHUNK_SIZE_PER_STREAM: usize = 20;

impl QuicProxyConnectionManager {
    pub async fn new(
        certificate: rustls::Certificate,
        key: rustls::PrivateKey,
        proxy_addr: SocketAddr,
    ) -> Self {
        info!("Configure Quic proxy connection manager to {}", proxy_addr);
        let endpoint = Self::create_proxy_client_endpoint(certificate, key);

        Self {
            endpoint,
            simple_thread_started: AtomicBool::from(false),
            proxy_addr,
            current_tpu_nodes: Arc::new(RwLock::new(vec![])),
            exit_signal: Arc::new(AtomicBool::from(false)),
        }
    }

    pub fn signal_shutdown(&self) {
        self.exit_signal.store(true, Relaxed);
    }

    pub async fn update_connection(
        &self,
        broadcast_receiver: Receiver<SentTransactionInfo>,
        // for duration of this slot these tpu nodes will receive the transactions
        connections_to_keep: HashMap<Pubkey, SocketAddr>,
        connection_parameters: QuicConnectionParameters,
    ) {
        debug!(
            "reconfigure quic proxy connection (# of tpu nodes: {})",
            connections_to_keep.len()
        );

        {
            let list_of_nodes = connections_to_keep
                .iter()
                .map(|(identity, tpu_address)| TpuNode {
                    tpu_identity: *identity,
                    tpu_address: *tpu_address,
                })
                .collect_vec();

            let mut lock = self.current_tpu_nodes.write().await;
            *lock = list_of_nodes;
        }

        if self.simple_thread_started.load(Relaxed) {
            // already started
            return;
        }
        self.simple_thread_started.store(true, Relaxed);

        info!("Starting very simple proxy thread");

        let exit_signal = self.exit_signal.clone();
        tokio::spawn(Self::read_transactions_and_broadcast(
            broadcast_receiver,
            self.current_tpu_nodes.clone(),
            self.proxy_addr,
            self.endpoint.clone(),
            exit_signal,
            connection_parameters,
        ));
    }

    fn create_proxy_client_endpoint(
        certificate: rustls::Certificate,
        key: rustls::PrivateKey,
    ) -> Endpoint {
        const ALPN_TPU_FORWARDPROXY_PROTOCOL_ID: &[u8] = b"solana-tpu-forward-proxy";

        let mut endpoint = {
            let client_socket = UdpSocket::bind("[::]:0").unwrap();
            let config = EndpointConfig::default();
            Endpoint::new(config, None, client_socket, Arc::new(TokioRuntime))
                .expect("create_endpoint quinn::Endpoint::new")
        };

        let mut crypto = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_client_auth_cert(vec![certificate], key)
            .expect("Failed to set QUIC client certificates");

        crypto.enable_early_data = true;
        crypto.alpn_protocols = vec![ALPN_TPU_FORWARDPROXY_PROTOCOL_ID.to_vec()];

        let mut config = ClientConfig::new(Arc::new(crypto));

        // note: this config must be aligned with quic-proxy's server config
        let mut transport_config = TransportConfig::default();
        // no remotely-initiated streams required
        transport_config.max_concurrent_uni_streams(VarInt::from_u32(0));
        transport_config.max_concurrent_bidi_streams(VarInt::from_u32(0));
        let timeout = Duration::from_secs(10).try_into().unwrap();
        transport_config.max_idle_timeout(Some(timeout));
        transport_config.keep_alive_interval(Some(Duration::from_millis(500)));
        apply_gso_workaround(&mut transport_config);

        config.transport_config(Arc::new(transport_config));
        endpoint.set_default_client_config(config);

        endpoint
    }

    // send transactions to quic proxy
    async fn read_transactions_and_broadcast(
        mut transaction_receiver: Receiver<SentTransactionInfo>,
        current_tpu_nodes: Arc<RwLock<Vec<TpuNode>>>,
        proxy_addr: SocketAddr,
        endpoint: Endpoint,
        exit_signal: Arc<AtomicBool>,
        connection_parameters: QuicConnectionParameters,
    ) {
        let auto_connection = AutoReconnect::new(endpoint, proxy_addr);

        loop {
            // exit signal set
            if exit_signal.load(Relaxed) {
                warn!("Caught exit signal - stopping sending transactions to quic proxy");
                break;
            }

            tokio::select! {
                tx = transaction_receiver.recv() => {

                    let first_tx: TxData = match tx {
                        Ok(SentTransactionInfo{
                            signature,
                            transaction,
                            ..
                        }) => {
                            TxData::new(signature, transaction)
                        },
                        Err(e) => {
                            warn!("Broadcast channel error (close) on recv: {} - aborting", e);
                            return;
                        }
                    };

                    let mut txs: Vec<TxData> = vec![first_tx];
                    for _ in 1..connection_parameters.number_of_transactions_per_unistream {
                        match transaction_receiver.try_recv() {
                            Ok(SentTransactionInfo{
                                signature,
                                transaction,
                                ..
                            }) => {
                                txs.push(TxData::new(signature, transaction));
                            },
                            Err(TryRecvError::Empty) => {
                                break;
                            }
                            Err(e) => {
                                warn!(
                                    "Broadcast channel error (close) on more recv: {} - aborting", e);
                                return;
                            }
                        };
                    }

                    let tpu_fanout_nodes = current_tpu_nodes.read().await.clone();

                    if tpu_fanout_nodes.is_empty() {
                        warn!("No tpu nodes to send transactions to - skip");
                        continue;
                    }

                    trace!("Sending copy of transaction batch of {} txs to {} tpu nodes via quic proxy",
                            txs.len(), tpu_fanout_nodes.len());

                    let send_result =
                        Self::send_copy_of_txs_to_quicproxy(
                            &txs, &auto_connection,
                            proxy_addr,
                            tpu_fanout_nodes)
                        .await;
                    if let Err(e) = send_result {
                        warn!("Failed to send copy of txs to quic proxy - skip (error {})", e);
                    }

                },
            }
        } // -- loop
    }

    async fn send_copy_of_txs_to_quicproxy(
        txs: &[TxData],
        auto_connection: &AutoReconnect,
        _proxy_address: SocketAddr,
        tpu_fanout_nodes: Vec<TpuNode>,
    ) -> anyhow::Result<()> {
        let tpu_data = tpu_fanout_nodes
            .iter()
            .map(|tpu| (tpu.tpu_address, tpu.tpu_identity))
            .collect_vec();

        for chunk in txs.chunks(CHUNK_SIZE_PER_STREAM) {
            let forwarding_request = TpuForwardingRequest::new(&tpu_data, chunk);
            debug!("forwarding_request: {}", forwarding_request);

            let proxy_request_raw =
                bincode::serialize(&forwarding_request).expect("Expect to serialize transactions");

            let send_result = auto_connection.send_uni(&proxy_request_raw).await;

            match send_result {
                Ok(()) => {
                    debug!("Successfully sent {} txs to quic proxy", txs.len());
                }
                Err(e) => {
                    bail!("Failed to send data to quic proxy: {:?}", e);
                }
            }
        } // -- one chunk

        Ok(())
    }
}
