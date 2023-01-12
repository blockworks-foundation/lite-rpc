use std::{
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};

use dashmap::DashMap;
use log::{info, warn};

use solana_client::nonblocking::tpu_client::TpuClient;

use crossbeam_channel::{Receiver, Sender};
use solana_transaction_status::TransactionStatus;
use tokio::task::JoinHandle;

pub type WireTransaction = Vec<u8>;

/// Retry transactions to a maximum of `u16` times, keep a track of confirmed transactions
#[derive(Clone)]
pub struct TxSender {
    /// Tx(s) forwarded to tpu
    pub txs_sent: Arc<DashMap<String, TxProps>>,
    /// Sender channel
    sender_channel: Sender<(String, Vec<u8>)>,
    /// Reciever channel
    recv_channel: Receiver<(String, Vec<u8>)>,
    /// TpuClient to call the tpu port
    tpu_client: Arc<TpuClient>,
    pub nb_tx_sent: Arc<AtomicU64>,
}

/// Transaction Properties
pub struct TxProps {
    pub status: Option<TransactionStatus>,
    pub sent_at: Instant,
}

impl Default for TxProps {
    fn default() -> Self {
        Self {
            status: Default::default(),
            sent_at: Instant::now(),
        }
    }
}

impl TxSender {
    pub fn new(tpu_client: Arc<TpuClient>) -> Self {
        let (sender, reciever) = crossbeam_channel::unbounded();
        Self {
            sender_channel: sender,
            recv_channel: reciever,
            tpu_client,
            txs_sent: Default::default(),
            nb_tx_sent: Arc::new(AtomicU64::new(0)),
        }
    }
    /// en-queue transaction if it doesn't already exist
    pub async fn enqnueue_tx(&self, sig: String, raw_tx: WireTransaction) {
        self.sender_channel.send((sig, raw_tx)).unwrap();
    }

    pub fn execute(
        self,

        tx_batch_size: usize,
        tx_send_interval: Duration,
    ) -> JoinHandle<anyhow::Result<()>> {
        tokio::spawn(async move {
            info!(
                "Batching tx(s) with batch size of {tx_batch_size} every {}ms",
                tx_send_interval.as_millis()
            );

            loop {
                let tpu_client = self.tpu_client.clone();
                let recv_res = self.recv_channel.recv();
                let txs_sent = self.txs_sent.clone();
                let nb_tx_sent = self.nb_tx_sent.clone();
                match recv_res {
                    Ok((signature, transaction)) => {
                        if tx_batch_size > 1 {
                            let mut transactions_vec = vec![transaction];
                            let mut signatures = vec![signature];
                            let mut time_remaining = Duration::from_micros(1000);
                            for _i in 1..tx_batch_size {
                                let start = std::time::Instant::now();
                                let another = self.recv_channel.recv_timeout(time_remaining);

                                match another {
                                    Ok((sig, tx)) => {
                                        transactions_vec.push(tx);
                                        signatures.push(sig);
                                    }
                                    Err(_) => break,
                                }
                                match time_remaining.checked_sub(start.elapsed()) {
                                    Some(x) => time_remaining = x,
                                    None => break,
                                }
                            }
                            tokio::spawn(async move {
                                let fut_res = tpu_client
                                    .try_send_wire_transaction_batch(transactions_vec)
                                    .await;

                                match fut_res {
                                    Ok(_) => {
                                        nb_tx_sent.fetch_add(
                                            signatures.len() as u64,
                                            std::sync::atomic::Ordering::Relaxed,
                                        );
                                        // insert sent transactions into signature status map
                                        signatures.iter().for_each(|signature| {
                                            txs_sent.insert(signature.clone(), TxProps::default());
                                        });
                                    }
                                    Err(e) => {
                                        warn!(
                                            "failed to send {} transactions error {}",
                                            signatures.len(),
                                            e
                                        );
                                        // insert sent transactions into signature status map
                                        signatures.iter().for_each(|signature| {
                                            txs_sent.insert(signature.clone(), TxProps{
                                                sent_at: Instant::now(),
                                                status: Some( TransactionStatus {
                                                    slot: 0,
                                                    confirmations: None,
                                                    status: Err(solana_sdk::transaction::TransactionError::ClusterMaintenance),
                                                    err: Some(solana_sdk::transaction::TransactionError::ClusterMaintenance),
                                                    confirmation_status: Some(solana_transaction_status::TransactionConfirmationStatus::Processed)
                                                })
                                            });
                                        });
                                    }
                                }
                            });
                        } else {
                            let sent_success = tpu_client.send_wire_transaction(transaction).await;
                            if sent_success {
                                nb_tx_sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                txs_sent.insert(signature, TxProps::default());
                            } else {
                                // insert sent transactions into signature status map
                                txs_sent.insert(signature.clone(), TxProps{
                                        sent_at: Instant::now(),
                                        status: Some( TransactionStatus {
                                            slot: 0,
                                            confirmations: None,
                                            status: Err(solana_sdk::transaction::TransactionError::ClusterMaintenance),
                                            err: Some(solana_sdk::transaction::TransactionError::ClusterMaintenance),
                                            confirmation_status: Some(solana_transaction_status::TransactionConfirmationStatus::Processed)
                                        })
                                    });
                            }
                        };
                    }
                    Err(e) => {
                        println!("got error on tpu channel {}", e.to_string());
                        break;
                    }
                }
            }
            // to give the correct type to JoinHandle
            Ok(())
        })
    }
}
