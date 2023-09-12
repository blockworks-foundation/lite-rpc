use async_trait::async_trait;
use solana_lite_rpc_core::block_storage_interface::BlockStorageInterface;
use solana_sdk::slot_history::Slot;
use solana_transaction_status::UiConfirmedBlock;
use std::collections::BTreeMap;
use tokio::sync::RwLock;

pub struct InmemoryBlockStore {
    block_storage: RwLock<BTreeMap<Slot, UiConfirmedBlock>>,
    number_of_blocks_to_store: usize,
}

impl InmemoryBlockStore {
    pub fn new(number_of_blocks_to_store: usize) -> Self {
        Self {
            number_of_blocks_to_store,
            block_storage: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn store(&self, slot: Slot, block: UiConfirmedBlock) -> Option<UiConfirmedBlock> {
        let mut block_storage = self.block_storage.write().await;
        let min_slot = match block_storage.first_key_value() {
            Some((slot, _)) => *slot,
            None => 0,
        };
        if slot > min_slot {
            block_storage.insert(slot, block);
            if block_storage.len() > self.number_of_blocks_to_store {
                block_storage.remove(&min_slot)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[async_trait]
impl BlockStorageInterface for InmemoryBlockStore {
    async fn save(
        &self,
        slot: Slot,
        block: UiConfirmedBlock,
        _commitment: solana_sdk::commitment_config::CommitmentLevel,
    ) {
        self.store(slot, block).await;
    }

    async fn get(&self, slot: Slot) -> Option<UiConfirmedBlock> {
        self.block_storage.read().await.get(&slot).cloned()
    }
}
