use crate::structures::produced_block::ProducedBlock;
use async_trait::async_trait;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::slot_history::Slot;
use std::{ops::Range, sync::Arc};

#[async_trait]
pub trait BlockStorageInterface: Send + Sync {
    // will save a block
    async fn save(&self, block: ProducedBlock);
    // will get a block
    async fn get(&self, slot: Slot, config: RpcBlockConfig) -> Option<ProducedBlock>;
    // will get range of slots that are stored in the storage
    async fn get_slot_range(&self) -> Range<Slot>;
}

pub type BlockStorageImpl = Arc<dyn BlockStorageInterface>;
