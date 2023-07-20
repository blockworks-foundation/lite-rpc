use std::cell::Cell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::atomic::Ordering::Relaxed;
use std::thread;
use std::time::Duration;
use anyhow::bail;
use async_trait::async_trait;
use itertools::Itertools;
use log::{debug, error, info, warn};
use quinn::Endpoint;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::Signer;
use solana_sdk::signature::Keypair;
use solana_sdk::transaction::VersionedTransaction;
use tokio::sync::{broadcast::Receiver, broadcast::Sender, RwLock};
use tokio::time::timeout;
use solana_lite_rpc_core::proxy_request_format::TpuForwardingRequest;
use solana_lite_rpc_core::quic_connection_utils::{QuicConnectionParameters, QuicConnectionUtils};
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
        let endpoint = QuicConnectionUtils::create_endpoint(certificate.clone(), key.clone());

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
            let mut lock = self.current_tpu_nodes.write().await;

            let list_of_nodes = connections_to_keep.iter().map(|(identity, tpu_address)| {
                TpuNode {
                    tpu_identity: identity.clone(),
                    tpu_address: tpu_address.clone(),
                }
            }).collect_vec();

            *lock = list_of_nodes;
        }


        if self.simple_thread_started.load(Relaxed) {
            // already started
            return;
        }

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

    // blocks and loops until exit signal (TODO)
    async fn read_transactions_and_broadcast(
        mut transaction_receiver: Receiver<(String, Vec<u8>)>,
        current_tpu_nodes: Arc<RwLock<Vec<TpuNode>>>,
        proxy_addr: SocketAddr,
        endpoint: Endpoint,
    ) {
        loop {
            // TODO exit signal ???

            tokio::select! {
                    // TODO add timeout
                    tx = transaction_receiver.recv() => {
                        // exit signal???


                        let the_tx: Vec<u8> = match tx {
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

                        // TODO read all txs from channel (see "let mut txs = vec![first_tx];")
                        let txs = vec![the_tx];

                        let tpu_fanout_nodes = current_tpu_nodes.read().await;

                        info!("Sending copy of transaction batch of {} to {} tpu nodes via quic proxy",
                                txs.len(), tpu_fanout_nodes.len());

                        for target_tpu_node in &*tpu_fanout_nodes {
                            Self::send_copy_of_txs_to_quicproxy(
                                &txs, endpoint.clone(),
                                proxy_addr,
                                target_tpu_node.tpu_address,
                                target_tpu_node.tpu_identity).await.unwrap();
                        }

                    },
                };
        }
    }

    async fn send_copy_of_txs_to_quicproxy(raw_tx_batch: &Vec<Vec<u8>>, endpoint: Endpoint,
                                           proxy_address: SocketAddr, tpu_target_address: SocketAddr,
                                           target_tpu_identity: Pubkey) -> anyhow::Result<()> {

        info!("sending vecvec: {}", raw_tx_batch.iter().map(|tx| tx.len()).into_iter().join(","));

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

        let proxy_request_raw = bincode::serialize(&forwarding_request).expect("Expect to serialize transactions");

        let send_result = timeout(Duration::from_millis(3500), Self::send_proxy_request(endpoint, proxy_address, &proxy_request_raw));

        match send_result.await {
            Ok(..) => {
                info!("Successfully sent data to quic proxy");
            }
            Err(e) => {
                warn!("Failed to send data to quic proxy: {:?}", e);
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


