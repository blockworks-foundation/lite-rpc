use crate::structures::produced_block::ProducedBlock;
use anyhow::Result;
use async_trait::async_trait;
use solana_sdk::slot_history::Slot;
use std::{ops::Range, sync::Arc};
use std::ops::{RangeInclusive, RangeToInclusive};

#[async_trait]
pub trait BlockStorageInterface: Send + Sync {
    // will save a block
    async fn save(&self, block: &ProducedBlock) -> Result<()>;
    // will get a block
    async fn get(&self, slot: Slot) -> Result<ProducedBlock>;
    // will get range of slots that are stored in the storage
    async fn get_slot_range(&self) -> RangeInclusive<Slot>;
}

pub type BlockStorageImpl = Arc<dyn BlockStorageInterface>;
