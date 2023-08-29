use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::slot_history::Slot;

use crate::structures::slot_notification::SlotNotification;
use crate::subscription_store::SubscriptionStore;
use crate::AtomicSlot;
use crate::{block_information_store::BlockInformationStore, tx_store::TxStore};

pub type TxSubKey = (String, CommitmentConfig);

#[derive(Default, Clone)]
pub struct SlotCache {
    current_slot: AtomicSlot,
    estimated_slot: AtomicSlot,
}

/// The central data store for all data from the cluster.
#[derive(Default, Clone)]
pub struct DataCache {
    pub block_store: BlockInformationStore,
    pub txs: TxStore,
    pub tx_subs: SubscriptionStore<TxSubKey>,
    pub slot_cache: SlotCache,
}

impl DataCache {
    pub async fn clean(&self, ttl_duration: std::time::Duration) {
        if let Some(latest_finalized_block) = self
            .block_store
            .get_latest_block(&CommitmentConfig::finalized())
            .await
        {
            self.block_store.clean().await;
            self.txs.clean(latest_finalized_block.block_height);
        }

        self.tx_subs.clean(ttl_duration);
    }
}

impl SlotCache {
    pub fn get_current_slot(&self) -> Slot {
        self.current_slot.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get_estimated_slot(&self) -> Slot {
        self.estimated_slot
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn update(&self, slot_notification: SlotNotification) {
        self.current_slot.store(
            slot_notification.processed_slot,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.estimated_slot.store(
            slot_notification.estimated_processed_slot,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}
