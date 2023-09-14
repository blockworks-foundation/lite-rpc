use async_trait::async_trait;
use solana_sdk::{commitment_config::CommitmentLevel, slot_history::Slot};
use solana_transaction_status::UiConfirmedBlock;
use std::sync::Arc;

#[async_trait]
pub trait BlockStorageInterface: Send + Sync {
    async fn save(&self, slot: Slot, block: UiConfirmedBlock, commitment: CommitmentLevel);
    async fn get(&self, slot: Slot) -> Option<UiConfirmedBlock>;
}

pub type BlockStorageImpl = Arc<dyn BlockStorageInterface>;
