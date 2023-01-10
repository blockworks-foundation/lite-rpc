use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use log::{info, warn};

use solana_client::nonblocking::tpu_client::TpuClient;

use tokio::task::JoinHandle;

use crate::{TxsSent, WireTransaction};

/// Retry transactions to a maximum of `u16` times, keep a track of confirmed transactions
#[derive(Clone)]
pub struct TxSender {
    /// Tx(s) forwarded to tpu
    pub txs_sent: TxsSent,
    /// Transactions queue for retrying
    enqueued_txs: Arc<RwLock<Vec<WireTransaction>>>,
    /// TpuClient to call the tpu port
    tpu_client: Arc<TpuClient>,
}

impl TxSender {
    pub fn new(tpu_client: Arc<TpuClient>) -> Self {
        Self {
            enqueued_txs: Default::default(),
            tpu_client,
            txs_sent: Default::default(),
        }
    }
    /// en-queue transaction if it doesn't already exist
    pub fn enqnueue_tx(&self, sig: String, raw_tx: WireTransaction) {
        self.txs_sent.insert(sig, None);
        self.enqueued_txs.write().unwrap().push(raw_tx);
    }

    /// retry enqued_tx(s)
    pub async fn retry_txs(&self, tx_batch_size: usize) {
        let mut enqueued_txs = Vec::new();

        std::mem::swap(&mut enqueued_txs, &mut self.enqueued_txs.write().unwrap());

        let enqueued_txs = self.enqueued_txs.read().unwrap().clone();

        let len = enqueued_txs.len();

        if len == 0 {
            return;
        }

        let mut tx_batch = Vec::with_capacity(len / tx_batch_size);

        let mut batch_index = 0;

        for (index, tx) in self.enqueued_txs.read().unwrap().iter().enumerate() {
            if index % tx_batch_size == 0 {
                tx_batch.push(Vec::with_capacity(tx_batch_size));
                batch_index += 1;
            }

            tx_batch[batch_index - 1].push(tx.to_owned());
        }

        for tx_batch in tx_batch {
            if let Err(err) = self
                .tpu_client
                .try_send_wire_transaction_batch(tx_batch)
                .await
            {
                warn!("{err}");
            }
        }
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
                self.retry_txs(tx_batch_size).await;
            }

            // to give the correct type to JoinHandle
            Ok(())
        })
    }
}
