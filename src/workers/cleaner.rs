use std::time::Duration;

use log::info;
use tokio::task::JoinHandle;

use super::{BlockListener, TxSender};

/// Background worker which cleans up memory  
#[derive(Clone)]
pub struct Cleaner<const N: usize> {
    tx_sender: TxSender,
    block_listeners: [BlockListener; N],
}

impl<const N: usize> Cleaner<N> {
    pub fn new(tx_sender: TxSender, block_listeners: [BlockListener; N]) -> Self {
        Self {
            tx_sender,
            block_listeners,
        }
    }

    pub fn clean_tx_sender(&self, ttl_duration: Duration) {
        let mut to_remove = vec![];

        for tx in self.tx_sender.txs_sent.iter() {
            if tx.sent_at.elapsed() >= ttl_duration {
                to_remove.push(tx.key().to_owned());
            }
        }

        for to_remove in &to_remove {
            self.tx_sender.txs_sent.remove(to_remove);
        }

        info!("Cleaned {} txs", to_remove.len());
    }

    /// Clean Signature Subscribers from Block Listeners
    pub fn clean_block_listeners(&self) {
        for block_listenser in &self.block_listeners {
            let mut to_remove = vec![];

            for subscriber in block_listenser.signature_subscribers.iter() {
                if subscriber.value().is_closed() {
                    to_remove.push(subscriber.key().to_owned());
                }
            }

            for to_remove in &to_remove {
                block_listenser.signature_subscribers.remove(to_remove);
            }

            info!("Cleaned {} Signature Subscribers", to_remove.len());
        }
    }

    pub fn start(self, ttl_duration: Duration) -> JoinHandle<anyhow::Result<()>> {
        let mut ttl = tokio::time::interval(ttl_duration);

        tokio::spawn(async move {
            info!("Cleaning memory");

            loop {
                ttl.tick().await;

                self.clean_tx_sender(ttl_duration);
                self.clean_block_listeners();
            }
        })
    }
}
