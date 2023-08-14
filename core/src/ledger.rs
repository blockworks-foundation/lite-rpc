use solana_sdk::commitment_config::CommitmentConfig;

use crate::subscription_store::SubscriptionStore;
use crate::{block_store::BlockStore, slot_clock::SlotClock, tx_store::TxStore};

pub type TxSubKey = (String, CommitmentConfig);

/// The central data store for all data from the cluster.
#[derive(Default, Clone)]
pub struct Ledger {
    pub block_store: BlockStore,
    pub clock: SlotClock,
    pub txs: TxStore,
    pub tx_subs: SubscriptionStore<TxSubKey>,
}

impl Ledger {
    pub async fn clean(&self, ttl_duration: std::time::Duration) {
        if let Some(latest_finalized_block) = self
            .block_store
            .get_latest_block_meta(&CommitmentConfig::finalized()).await
        {
            self.block_store.clean().await;
            self.txs.clean(latest_finalized_block.block_height);
        }

        self.tx_subs.clean(ttl_duration);
    }
}
