use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use log::{info, warn};

use solana_transaction_status::TransactionStatus;
use tokio::{sync::RwLock, task::JoinHandle};

use crate::tpu_manager::TpuManager;

pub type WireTransaction = Vec<u8>;

/// Retry transactions to a maximum of `u16` times, keep a track of confirmed transactions
#[derive(Clone)]
pub struct TxSender {
    /// Tx(s) forwarded to tpu
    pub txs_sent: Arc<DashMap<String, TxProps>>,
    /// Transactions queue for retrying
    enqueued_txs: Arc<RwLock<Vec<(String, WireTransaction, u64)>>>,
    /// TpuClient to call the tpu port
    tpu_manager: Arc<TpuManager>,
}

/// Transaction Properties
pub struct TxProps {
    pub status: Option<TransactionStatus>,
    /// Time at which transaction was forwarded
    pub sent_at: Instant,
    /// slot corresponding to the recent blockhash of the tx
    pub recent_slot: u64,
    /// current slot estimated by the quic client when forwarding
    pub forwarded_slot: u64,
}

impl Default for TxProps {
    fn default() -> Self {
        Self {
            status: Default::default(),
            sent_at: Instant::now(),
            recent_slot: Default::default(),
            forwarded_slot: Default::default(),
        }
    }
}

impl TxSender {
    pub fn new(tpu_manager: Arc<TpuManager>) -> Self {
        Self {
            enqueued_txs: Default::default(),
            tpu_manager,
            txs_sent: Default::default(),
        }
    }

    /// en-queue transaction if it doesn't already exist
    pub async fn enqnueue_tx(&self, sig: String, raw_tx: WireTransaction, recent_slot: u64) {
        self.enqueued_txs
            .write()
            .await
            .push((sig, raw_tx, recent_slot));
    }

    /// retry enqued_tx(s)
    pub async fn forward_txs(&self, tx_batch_size: usize) {
        if self.enqueued_txs.read().await.is_empty() {
            return;
        }

        let mut enqueued_txs = Vec::new();
        std::mem::swap(&mut enqueued_txs, &mut *self.enqueued_txs.write().await);

        let mut tx_remaining = enqueued_txs.len();
        let mut enqueued_txs = enqueued_txs.into_iter();
        let tpu_client = self.tpu_manager.clone();
        let txs_sent = self.txs_sent.clone();

        tokio::spawn(async move {
            while tx_remaining != 0 {
                let mut batch = Vec::with_capacity(tx_batch_size);
                let mut sigs_and_slots = Vec::with_capacity(tx_batch_size);

                for (batched, (sig, tx, recent_slot)) in enqueued_txs.by_ref().enumerate() {
                    batch.push(tx);
                    sigs_and_slots.push((sig, recent_slot));
                    tx_remaining -= 1;
                    if batched == tx_batch_size {
                        break;
                    }
                }

                match tpu_client.try_send_wire_transaction_batch(batch).await {
                    Ok(_) => {
                        for (sig, recent_slot) in sigs_and_slots {
                            txs_sent.insert(
                                sig,
                                TxProps {
                                    recent_slot,
                                    ..Default::default()
                                },
                            );
                        }
                    }
                    Err(err) => {
                        warn!("{err}");
                    }
                }
            }
        });
    }

    /// retry and confirm transactions every 2ms (avg time to confirm tx)
    pub fn execute(
        self,
        tx_batch_size: usize,
        tx_send_interval: Duration,
    ) -> JoinHandle<anyhow::Result<()>> {
        let mut interval = tokio::time::interval(tx_send_interval);

        #[allow(unreachable_code)]
        tokio::spawn(async move {
            info!(
                "Batching tx(s) with batch size of {tx_batch_size} every {}ms",
                tx_send_interval.as_millis()
            );

            loop {
                interval.tick().await;
                self.forward_txs(tx_batch_size).await;
            }

            // to give the correct type to JoinHandle
            Ok(())
        })
    }
}
