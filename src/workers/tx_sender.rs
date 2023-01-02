use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use log::{info, warn};

use solana_client::nonblocking::tpu_client::TpuClient;

use solana_sdk::signature::Signature;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::{WireTransaction, DEFAULT_TX_RETRY_BATCH_SIZE, TX_MAX_RETRIES_UPPER_LIMIT};

use super::block_listenser::BlockListener;

/// Retry transactions to a maximum of `u16` times, keep a track of confirmed transactions
#[derive(Clone)]
pub struct TxSender {
    /// Transactions queue for retrying
    enqueued_txs: Arc<RwLock<HashMap<Signature, (WireTransaction, u16)>>>,
    /// block_listner
    block_listner: BlockListener,
    /// TpuClient to call the tpu port
    tpu_client: Arc<TpuClient>,
}

impl TxSender {
    pub fn new(tpu_client: Arc<TpuClient>, block_listner: BlockListener) -> Self {
        Self {
            enqueued_txs: Default::default(),
            block_listner,
            tpu_client,
        }
    }
    /// en-queue transaction if it doesn't already exist
    pub async fn enqnueue_tx(&self, sig: Signature, raw_tx: WireTransaction, max_retries: u16) {
        if max_retries == 0 {
            return;
        }

        if !self.block_listner.blocks.contains_key(&sig.to_string()) {
            let max_retries = max_retries.min(TX_MAX_RETRIES_UPPER_LIMIT);
            info!("en-queuing {sig} with max retries {max_retries}");
            self.enqueued_txs
                .write()
                .await
                .insert(sig, (raw_tx, max_retries));

            println!("{:?}", self.enqueued_txs.read().await.len());
        }
    }

    /// retry enqued_tx(s)
    pub async fn retry_txs(&self) {
        let len = self.enqueued_txs.read().await.len();

        info!("retrying {len} tx(s)");

        if len == 0 {
            return;
        }

        let mut enqued_tx = self.enqueued_txs.write().await;

        let mut tx_batch = Vec::with_capacity(enqued_tx.len() / DEFAULT_TX_RETRY_BATCH_SIZE);
        let mut stale_txs = vec![];

        let mut batch_index = 0;

        for (index, (sig, (tx, retries))) in enqued_tx.iter_mut().enumerate() {
            if self.block_listner.blocks.contains_key(&sig.to_string()) {
                stale_txs.push(sig.to_owned());
                continue;
            }

            if index % DEFAULT_TX_RETRY_BATCH_SIZE == 0 {
                tx_batch.push(Vec::with_capacity(DEFAULT_TX_RETRY_BATCH_SIZE));
                batch_index += 1;
            }

            tx_batch[batch_index - 1].push(tx.clone());

            let Some(retries_left) = retries.checked_sub(1) else {
                stale_txs.push(sig.to_owned());
                continue;
            };

            info!("retrying {sig} with {retries_left} retries left");

            *retries = retries_left;
        }

        // remove stale tx(s)
        for stale_tx in stale_txs {
            enqued_tx.remove(&stale_tx);
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

    /// retry and confirm transactions every 800ms (avg time to confirm tx)
    pub fn execute(self) -> JoinHandle<anyhow::Result<()>> {
        let mut interval = tokio::time::interval(Duration::from_millis(800));

        #[allow(unreachable_code)]
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                self.retry_txs().await;
            }

            // to give the correct type to JoinHandle
            Ok(())
        })
    }
}
