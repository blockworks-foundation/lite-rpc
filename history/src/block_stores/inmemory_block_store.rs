use async_trait::async_trait;
use solana_lite_rpc_core::{
    commitment_utils::Commitment,
    structures::produced_block::ProducedBlock,
    traits::block_storage_interface::{BlockStorageInterface, BLOCK_NOT_FOUND},
};
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::slot_history::Slot;
use std::{collections::BTreeMap, ops::Range};
use tokio::sync::RwLock;

pub struct InmemoryBlockStore {
    block_storage: RwLock<BTreeMap<Slot, ProducedBlock>>,
    number_of_blocks_to_store: usize,
}

impl InmemoryBlockStore {
    pub fn new(number_of_blocks_to_store: usize) -> Self {
        Self {
            number_of_blocks_to_store,
            block_storage: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn store(&self, block: &ProducedBlock) {
        let slot = block.slot;
        let mut block_storage = self.block_storage.write().await;
        let min_slot = match block_storage.first_key_value() {
            Some((slot, _)) => *slot,
            None => 0,
        };
        if slot >= min_slot {
            // overwrite block only if confirmation has changed
            match block_storage.get_mut(&slot) {
                Some(x) => {
                    let commitment_store = Commitment::from(x.commitment_config);
                    let commitment_block = Commitment::from(block.commitment_config);
                    let overwrite = commitment_block > commitment_store;
                    if overwrite {
                        *x = block.clone();
                    }
                }
                None => {
                    block_storage.insert(slot, block.clone());
                }
            }
            if block_storage.len() > self.number_of_blocks_to_store {
                block_storage.remove(&min_slot);
            }
        }
    }
}

#[async_trait]
impl BlockStorageInterface for InmemoryBlockStore {
    async fn save(&self, block: &ProducedBlock) -> anyhow::Result<()> {
        self.store(block).await;
        Ok(())
    }

    async fn get(&self, slot: Slot, _: RpcBlockConfig) -> anyhow::Result<ProducedBlock> {
        self.block_storage
            .read()
            .await
            .get(&slot)
            .cloned()
            .ok_or(anyhow::Error::msg(BLOCK_NOT_FOUND))
    }

    async fn get_slot_range(&self) -> Range<Slot> {
        let lk = self.block_storage.read().await;
        let first = lk.first_key_value();
        let last = lk.last_key_value();
        if let Some((first_slot, _)) = first {
            let Some((last_slot, _)) = last else {
                return Range::default();
            };
            *first_slot..(*last_slot + 1)
        } else {
            Range::default()
        }
    }
}
