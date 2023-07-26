use std::cell::Cell;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::atomic::Ordering::Relaxed;
use std::thread;
use std::time::Duration;
use anyhow::{bail, Context};
use async_trait::async_trait;
use futures::FutureExt;
use itertools::Itertools;
use log::{debug, error, info, warn};
use quinn::{ClientConfig, Connection, Endpoint, EndpointConfig, IdleTimeout, TokioRuntime, TransportConfig, VarInt};
use solana_sdk::packet::PACKET_DATA_SIZE;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::Signer;
use solana_sdk::signature::Keypair;
use solana_sdk::transaction::VersionedTransaction;
use tokio::sync::{broadcast::Receiver, broadcast::Sender, RwLock};
use tokio::time::timeout;
use solana_lite_rpc_core::proxy_request_format::TpuForwardingRequest;
use solana_lite_rpc_core::quic_connection_utils::{QuicConnectionParameters, QuicConnectionUtils, SkipServerVerification};
use solana_lite_rpc_core::structures::identity_stakes::IdentityStakes;
use solana_lite_rpc_core::tx_store::TxStore;

#[derive(Clone, Copy, Debug)]
pub struct TpuNode {
    pub tpu_identity: Pubkey,
    pub tpu_address: SocketAddr,
}

pub struct QuicProxyConnectionManager {
    endpoint: Endpoint,
    validator_identity: Arc<Keypair>,
    simple_thread_started: AtomicBool,
    proxy_addr: SocketAddr,
    current_tpu_nodes: Arc<RwLock<Vec<TpuNode>>>
}

impl QuicProxyConnectionManager {
    pub async fn new(
        certificate: rustls::Certificate,
        key: rustls::PrivateKey,
        validator_identity: Arc<Keypair>,
        proxy_addr: SocketAddr,
    ) -> Self {
        let endpoint = Self::create_proxy_client_endpoint(certificate.clone(), key.clone());

        Self {
            endpoint,
            validator_identity,
            simple_thread_started: AtomicBool::from(false),
            proxy_addr,
            current_tpu_nodes: Arc::new(RwLock::new(vec![])),
        }
    }

    pub async fn update_connection(
        &self,
        transaction_sender: Arc<Sender<(String, Vec<u8>)>>,
        // for duration of this slot these tpu nodes will receive the transactions
        connections_to_keep: HashMap<Pubkey, SocketAddr>,
    ) {
        debug!("reconfigure quic proxy connection (# of tpu nodes: {})", connections_to_keep.len());


        {
            let list_of_nodes = connections_to_keep.iter().map(|(identity, tpu_address)| {
                TpuNode {
                    tpu_identity: identity.clone(),
                    tpu_address: tpu_address.clone(),
                }
            }).collect_vec();

            let mut lock = self.current_tpu_nodes.write().await;
            *lock = list_of_nodes;
        }


        if self.simple_thread_started.load(Relaxed) {
            // already started
            return;
        }
        self.simple_thread_started.store(true, Relaxed);

        info!("Starting very simple proxy thread");

        let mut transaction_receiver = transaction_sender.subscribe();

        // TODO

        tokio::spawn(Self::read_transactions_and_broadcast(
            transaction_receiver,
            self.current_tpu_nodes.clone(),
            self.proxy_addr,
            self.endpoint.clone(),
        ));

    }

    fn create_proxy_client_endpoint(certificate: rustls::Certificate, key: rustls::PrivateKey) -> Endpoint {

        const ALPN_TPU_FORWARDPROXY_PROTOCOL_ID: &[u8] = b"solana-tpu-forward-proxy";

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
        crypto.alpn_protocols = vec![ALPN_TPU_FORWARDPROXY_PROTOCOL_ID.to_vec()];

        let mut config = ClientConfig::new(Arc::new(crypto));

        // note: this config must be aligned with quic-proxy's server config
        let mut transport_config = TransportConfig::default();
        let timeout = IdleTimeout::try_from(Duration::from_secs(1)).unwrap();
        // no remotely-initiated streams required
        transport_config.max_concurrent_uni_streams(VarInt::from_u32(0));
        transport_config.max_concurrent_bidi_streams(VarInt::from_u32(0));
        let timeout = Duration::from_secs(10).try_into().unwrap();
        transport_config.max_idle_timeout(Some(timeout));
        transport_config.keep_alive_interval(Some(Duration::from_millis(500)));

        config.transport_config(Arc::new(transport_config));
        endpoint.set_default_client_config(config);

        endpoint
    }

    // blocks and loops until exit signal (TODO)
    async fn read_transactions_and_broadcast(
        mut transaction_receiver: Receiver<(String, Vec<u8>)>,
        current_tpu_nodes: Arc<RwLock<Vec<TpuNode>>>,
        proxy_addr: SocketAddr,
        endpoint: Endpoint,
    ) {

        let mut connection = endpoint.connect(proxy_addr, "localhost").unwrap()
            .await.unwrap();

        loop {
            // TODO exit signal ???

            tokio::select! {
                    // TODO add timeout
                    tx = transaction_receiver.recv() => {
                        // exit signal???


                        let first_tx: Vec<u8> = match tx {
                            Ok((sig, tx)) => {
                                // if Self::check_for_confirmation(&txs_sent_store, sig) {
                                //     // transaction is already confirmed/ no need to send
                                //     continue;
                                // }
                                tx
                            },
                            Err(e) => {
                                error!(
                                    "Broadcast channel error on recv error {}", e);
                                continue;
                            }
                        };

                        let number_of_transactions_per_unistream = 8; // TODO read from QuicConnectionParameters

                        let mut txs = vec![first_tx];
                        // TODO comment in
                        let foo = PACKET_DATA_SIZE;
                        for _ in 1..number_of_transactions_per_unistream {
                            if let Ok((signature, tx)) = transaction_receiver.try_recv() {
                                // if Self::check_for_confirmation(&txs_sent_store, signature) {
                                //     continue;
                                // }
                                txs.push(tx);
                            }
                        }

                        let tpu_fanout_nodes = current_tpu_nodes.read().await.clone();

                        info!("Sending copy of transaction batch of {} txs to {} tpu nodes via quic proxy",
                                txs.len(), tpu_fanout_nodes.len());

                        for target_tpu_node in tpu_fanout_nodes {
                            Self::send_copy_of_txs_to_quicproxy(
                                &txs, endpoint.clone(),
                            proxy_addr,
                                target_tpu_node.tpu_address,
                                target_tpu_node.tpu_identity)
                            .await.unwrap();
                        }

                    },
                };
        }
    }

    async fn send_copy_of_txs_to_quicproxy(raw_tx_batch: &Vec<Vec<u8>>, endpoint: Endpoint,
                                           proxy_address: SocketAddr, tpu_target_address: SocketAddr,
                                           target_tpu_identity: Pubkey) -> anyhow::Result<()> {

        info!("sending vecvec {} to quic proxy for TPU node {}",
            raw_tx_batch.iter().map(|tx| tx.len()).into_iter().join(","), tpu_target_address);

        // TODO add timeout
        // let mut send_stream = timeout(Duration::from_millis(500), connection.open_uni()).await??;

        let raw_tx_batch_copy = raw_tx_batch.clone();

        let mut txs = vec![];

        for raw_tx in raw_tx_batch_copy {
            let tx = match bincode::deserialize::<VersionedTransaction>(&raw_tx) {
                Ok(tx) => tx,
                Err(err) => {
                    bail!(err.to_string());
                }
            };
            txs.push(tx);
        }

        let forwarding_request = TpuForwardingRequest::new(tpu_target_address, target_tpu_identity, txs);
        debug!("forwarding_request: {}", forwarding_request);

        let proxy_request_raw = bincode::serialize(&forwarding_request).expect("Expect to serialize transactions");

        let send_result =
            timeout(Duration::from_millis(3500), Self::send_proxy_request(endpoint, proxy_address, &proxy_request_raw))
                .await.context("Timeout sending data to quic proxy")?;

        match send_result {
            Ok(()) => {
                info!("Successfully sent data to quic proxy");
            }
            Err(e) => {
                bail!("Failed to send data to quic proxy: {:?}", e);
            }
        }

        Ok(())
    }

     async fn send_proxy_request(endpoint: Endpoint, proxy_address: SocketAddr, proxy_request_raw: &Vec<u8>) -> anyhow::Result<()> {
         info!("sending {} bytes to proxy", proxy_request_raw.len());

         let mut connecting = endpoint.connect(proxy_address, "localhost")?;
         let connection = timeout(Duration::from_millis(500), connecting).await??;
         let mut send = connection.open_uni().await?;

             send.write_all(proxy_request_raw).await?;

             send.finish().await?;

             Ok(())
     }


}


