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
            block_store,
        }
    }

    pub fn clean_tx_sender(&self, ttl_duration: Duration) {
        self.tx_sender.cleanup(ttl_duration);
    }

    /// Clean Signature Subscribers from Block Listeners
    pub fn clean_block_listeners(&self, ttl_duration: Duration) {
        self.block_listenser.clean(ttl_duration);
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
                self.clean_block_listeners(ttl_duration);
                self.clean_block_store(ttl_duration).await;
            }
        })
    }
}
