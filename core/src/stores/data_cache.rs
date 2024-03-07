use crate::structures::leaderschedule::CalculatedSchedule;
use dashmap::DashMap;
use solana_sdk::hash::Hash;
use solana_sdk::slot_history::Slot;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::sync::{atomic::AtomicU64, Arc};
use tokio::sync::RwLock;

use crate::{
    stores::{
        block_information_store::BlockInformationStore, cluster_info_store::ClusterInfo,
        subscription_store::SubscriptionStore, tx_store::TxStore,
    },
    structures::{
        epoch::{Epoch, EpochCache},
        identity_stakes::IdentityStakes,
        slot_notification::{AtomicSlot, SlotNotification},
        transaction_sent_info::SentTransactionInfo,
    },
};

use super::block_information_store::BlockInformation;
pub type TxSubKey = (String, CommitmentConfig);

#[derive(Default, Clone)]
pub struct SlotCache {
    current_slot: AtomicSlot,
    estimated_slot: AtomicSlot,
}

/// The central data store for all data from the cluster.
#[derive(Clone)]
pub struct DataCache {
    pub block_information_store: BlockInformationStore,
    pub txs: TxStore,
    pub tx_subs: SubscriptionStore,
    pub slot_cache: SlotCache,
    pub identity_stakes: IdentityStakes,
    pub cluster_info: ClusterInfo,
    pub epoch_data: EpochCache,
    pub leader_schedule: Arc<RwLock<CalculatedSchedule>>,
}

impl DataCache {
    pub async fn clean(&self, ttl_duration: std::time::Duration) {
        let block_info = self
            .block_information_store
            .get_latest_block_info(CommitmentConfig::finalized())
            .await;
        self.block_information_store.clean().await;
        self.txs.clean(block_info.block_height);

        self.tx_subs.clean(ttl_duration);
    }

    pub async fn check_if_confirmed_or_expired_blockheight(
        &self,
        sent_transaction_info: &SentTransactionInfo,
    ) -> bool {
        let last_block = self
            .block_information_store
            .get_latest_block_info(CommitmentConfig::processed())
            .await;
        self.txs
            .is_transaction_confirmed(&sent_transaction_info.signature)
            || last_block.block_height > sent_transaction_info.last_valid_block_height
    }

    pub async fn get_current_epoch(&self, commitment: CommitmentConfig) -> Epoch {
        let BlockInformation { slot, .. } = self
            .block_information_store
            .get_latest_block(commitment)
            .await;
        self.epoch_data.get_epoch_at_slot(slot)
    }

    pub fn new_for_tests() -> Self {
        Self {
            block_information_store: BlockInformationStore::new(BlockInformation {
                block_height: 0,
                blockhash: Hash::new_unique().to_string(),
                cleanup_slot: 1000,
                commitment_config: CommitmentConfig::finalized(),
                last_valid_blockheight: 300,
                slot: 0,
            }),
            cluster_info: ClusterInfo::default(),
            identity_stakes: IdentityStakes::new(Pubkey::new_unique()),
            slot_cache: SlotCache::new(0),
            tx_subs: SubscriptionStore::default(),
            txs: TxStore {
                store: Arc::new(DashMap::new()),
            },
            epoch_data: EpochCache::new_for_tests(),
            leader_schedule: Arc::new(RwLock::new(CalculatedSchedule::default())),
        }
    }
}

impl SlotCache {
    pub fn new(slot: Slot) -> Self {
        Self {
            current_slot: Arc::new(AtomicU64::new(slot)),
            estimated_slot: Arc::new(AtomicU64::new(slot)),
        }
    }
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
