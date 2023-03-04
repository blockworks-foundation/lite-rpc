use std::time::Duration;

use log::info;
use tokio::task::JoinHandle;

use crate::block_store::BlockStore;

use super::{BlockListener, TxSender};

/// Background worker which cleans up memory  
#[derive(Clone)]
pub struct Cleaner {
    tx_sender: TxSender,
    block_listenser: BlockListener,
    block_store: BlockStore,
}

impl Cleaner {
    pub fn new(
        tx_sender: TxSender,
        block_listenser: BlockListener,
        block_store: BlockStore,
    ) -> Self {
        Self {
            tx_sender,
            block_listenser,
            block_store: block_store,
        }
    }

    pub fn clean_tx_sender(&self, ttl_duration: Duration) {
        let length_before = self.tx_sender.txs_sent.len();
        self.tx_sender
            .txs_sent
            .retain(|_k, v| v.sent_at.elapsed() < ttl_duration);
        info!(
            "Cleaned {} transactions",
            length_before - self.tx_sender.txs_sent.len()
        );
    }

    /// Clean Signature Subscribers from Block Listeners
    pub fn clean_block_listeners(&self) {
        let length_before = self.block_listenser.signature_subscribers.len();
        self.block_listenser
            .signature_subscribers
            .retain(|_k, v| !v.is_closed());

        info!(
            "Cleaned {} Signature Subscribers",
            length_before - self.block_listenser.signature_subscribers.len()
        );
    }

    pub async fn clean_block_store(&self, ttl_duration: Duration) {
        self.block_store.clean(ttl_duration).await;
    }

    pub fn start(self, ttl_duration: Duration) -> JoinHandle<anyhow::Result<()>> {
        let mut ttl = tokio::time::interval(ttl_duration);

        tokio::spawn(async move {
            info!("Cleaning memory");

            loop {
                ttl.tick().await;

                self.clean_tx_sender(ttl_duration);
                self.clean_block_listeners();
                self.clean_block_store(ttl_duration).await;
            }
        })
    }
}
