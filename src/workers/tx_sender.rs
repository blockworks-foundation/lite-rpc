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
    enqueued_txs: Arc<RwLock<Vec<(String, WireTransaction)>>>,
    /// TpuClient to call the tpu port
    tpu_manager: Arc<TpuManager>,
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
    pub fn new(tpu_manager: Arc<TpuManager>) -> Self {
        Self {
            enqueued_txs: Default::default(),
            tpu_manager,
            txs_sent: Default::default(),
        }
    }
    /// en-queue transaction if it doesn't already exist
    pub async fn enqnueue_tx(&self, sig: String, raw_tx: WireTransaction) {
        self.enqueued_txs.write().await.push((sig, raw_tx));
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
                let mut sigs = Vec::with_capacity(tx_batch_size);

                for (batched, (sig, tx)) in enqueued_txs.by_ref().enumerate() {
                    batch.push(tx);
                    sigs.push(sig);
                    tx_remaining -= 1;
                    if batched == tx_batch_size {
                        break;
                    }
                }

                match tpu_client.try_send_wire_transaction_batch(batch).await {
                    Ok(_) => {
                        for sig in sigs {
                            txs_sent.insert(sig, TxProps::default());
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
