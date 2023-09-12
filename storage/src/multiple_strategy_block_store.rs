// A mixed block store,
// Stores confirmed blocks in memory
// Finalized blocks in long term storage of your choice
// Fetches legacy blocks from faithful

use crate::block_stores::inmemory_block_store::InmemoryBlockStore;
use async_trait::async_trait;
use solana_lite_rpc_core::block_storage_interface::{BlockStorageImpl, BlockStorageInterface};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    slot_history::Slot,
};
use solana_transaction_status::{TransactionDetails, UiConfirmedBlock, UiTransactionEncoding};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub struct MultipleStrategyBlockStorage {
    inmemory_for_storage: InmemoryBlockStore, // for confirmed blocks
    persistent_block_storage: BlockStorageImpl, // for persistent block storage
    faithful_rpc_client: Arc<RpcClient>,      // to fetch legacy blocks from faithful
    number_of_slots_in_memory: usize,
    last_confirmed_slot: Arc<AtomicU64>,
}

impl MultipleStrategyBlockStorage {
    pub fn new(
        persistent_block_storage: BlockStorageImpl,
        faithful_url: String,
        number_of_slots_in_memory: usize,
    ) -> Self {
        Self {
            inmemory_for_storage: InmemoryBlockStore::new(number_of_slots_in_memory),
            persistent_block_storage,
            faithful_rpc_client: Arc::new(RpcClient::new(faithful_url)),
            number_of_slots_in_memory,
            last_confirmed_slot: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn get_in_memory_block(&self, slot: Slot) -> Option<UiConfirmedBlock> {
        self.inmemory_for_storage.get(slot).await
    }
}

#[async_trait]
impl BlockStorageInterface for MultipleStrategyBlockStorage {
    async fn save(
        &self,
        slot: solana_sdk::slot_history::Slot,
        block: solana_transaction_status::UiConfirmedBlock,
        commitment: solana_sdk::commitment_config::CommitmentLevel,
    ) {
        if commitment == CommitmentLevel::Confirmed {
            self.inmemory_for_storage
                .save(slot, block, commitment)
                .await;
        } else if commitment == CommitmentLevel::Finalized {
            let block_in_mem = self.inmemory_for_storage.get(slot).await;
            if block_in_mem.is_none() {
                self.inmemory_for_storage
                    .save(slot, block.clone(), commitment)
                    .await;
            } else if let Some(block_in_mem) = block_in_mem {
                // check if inmemory blockhash is same as finalized, update it if they are not
                // we can have two machines with same identity publishing two different blocks on same slot
                if block_in_mem.blockhash != block.blockhash {
                    self.inmemory_for_storage
                        .save(slot, block.clone(), commitment)
                        .await;
                }
            }
            self.persistent_block_storage
                .save(slot, block, commitment)
                .await;
        }
        if slot > self.last_confirmed_slot.load(Ordering::Relaxed) {
            self.last_confirmed_slot.store(slot, Ordering::Relaxed);
        }
    }

    async fn get(
        &self,
        slot: solana_sdk::slot_history::Slot,
    ) -> Option<solana_transaction_status::UiConfirmedBlock> {
        let last_confirmed_slot = self.last_confirmed_slot.load(Ordering::Relaxed);
        if slot > last_confirmed_slot {
            None
        } else {
            let diff = last_confirmed_slot.saturating_sub(slot);
            if diff < self.number_of_slots_in_memory as u64 {
                let block = self.inmemory_for_storage.get(slot).await;
                if block.is_some() {
                    return block;
                }
            }
            // TODO: Define what data is expected that is definetly not in persistant block storage like data after epoch - 1
            // check persistant block
            let persistant_block = self.persistent_block_storage.get(slot).await;
            if persistant_block.is_some() {
                persistant_block
            } else {
                // fetch from faithful
                self.faithful_rpc_client
                    .get_block_with_config(
                        slot,
                        RpcBlockConfig {
                            transaction_details: Some(TransactionDetails::Full),
                            commitment: Some(CommitmentConfig::finalized()),
                            max_supported_transaction_version: Some(0),
                            encoding: Some(UiTransactionEncoding::Base64),
                            rewards: Some(true),
                        },
                    )
                    .await
                    .ok()
            }
        }
    }
}
