// A mixed block store,
// Stores confirmed blocks in memory
// Finalized blocks in long term storage of your choice
// Fetches legacy blocks from faithful

use crate::block_stores::inmemory_block_store::InmemoryBlockStore;
use anyhow::{bail, Result};
use async_trait::async_trait;
use solana_lite_rpc_core::{
    commitment_utils::Commitment,
    structures::produced_block::ProducedBlock,
    traits::block_storage_interface::{BlockStorageImpl, BlockStorageInterface, BLOCK_NOT_FOUND},
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};
use std::{
    ops::Range,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use solana_lite_rpc_cluster_endpoints::rpc_polling::poll_blocks::from_ui_block;

pub struct MultipleStrategyBlockStorage {
    inmemory_for_storage: InmemoryBlockStore, // for confirmed blocks
    persistent_block_storage: BlockStorageImpl, // for persistent block storage
    faithful_rpc_client: Option<Arc<RpcClient>>, // to fetch legacy blocks from faithful
    last_confirmed_slot: Arc<AtomicU64>,
}

impl MultipleStrategyBlockStorage {
    pub fn new(
        persistent_block_storage: BlockStorageImpl,
        faithful_rpc_client: Option<Arc<RpcClient>>,
        number_of_slots_in_memory: usize,
    ) -> Self {
        Self {
            inmemory_for_storage: InmemoryBlockStore::new(number_of_slots_in_memory),
            persistent_block_storage,
            faithful_rpc_client,
            last_confirmed_slot: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn get_in_memory_block(&self, slot: Slot) -> anyhow::Result<ProducedBlock> {
        self.inmemory_for_storage
            .get(
                slot,
                RpcBlockConfig {
                    encoding: None,
                    transaction_details: None,
                    rewards: None,
                    commitment: None,
                    max_supported_transaction_version: None,
                },
            )
            .await
    }
}

#[async_trait]
impl BlockStorageInterface for MultipleStrategyBlockStorage {
    async fn save(&self, block: ProducedBlock) -> Result<()> {
        let slot = block.slot;
        let commitment = Commitment::from(block.commitment_config);
        match commitment {
            Commitment::Confirmed | Commitment::Processed => {
                self.inmemory_for_storage.save(block).await?;
            }
            Commitment::Finalized => {
                let block_in_mem = self.get_in_memory_block(block.slot).await;
                match block_in_mem {
                    Ok(block_in_mem) => {
                        // check if inmemory blockhash is same as finalized, update it if they are not
                        // we can have two machines with same identity publishing two different blocks on same slot
                        if block_in_mem.blockhash != block.blockhash {
                            self.inmemory_for_storage.save(block.clone()).await?;
                        }
                    }
                    Err(_) => self.inmemory_for_storage.save(block.clone()).await?,
                }
                self.persistent_block_storage.save(block).await?;
            }
        };
        if slot > self.last_confirmed_slot.load(Ordering::Relaxed) {
            self.last_confirmed_slot.store(slot, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn get(
        &self,
        slot: solana_sdk::slot_history::Slot,
        config: RpcBlockConfig,
    ) -> Result<ProducedBlock> {
        let last_confirmed_slot = self.last_confirmed_slot.load(Ordering::Relaxed);
        if slot > last_confirmed_slot {
            bail!(BLOCK_NOT_FOUND);
        } else {
            let range = self.inmemory_for_storage.get_slot_range().await;
            if range.contains(&slot) {
                let block = self.inmemory_for_storage.get(slot, config).await;
                if block.is_ok() {
                    return block;
                }
            }
            // TODO: Define what data is expected that is definetly not in persistant block storage like data after epoch - 1
            // check persistant block
            let persistent_block_range = self.persistent_block_storage.get_slot_range().await;
            if persistent_block_range.contains(&slot) {
                self.persistent_block_storage.get(slot, config).await
            } else if let Some(faithful_rpc_client) = self.faithful_rpc_client.clone() {
                match faithful_rpc_client
                    .get_block_with_config(slot, config)
                    .await
                {
                    Ok(block) => Ok(from_ui_block(
                        block,
                        slot,
                        CommitmentConfig::finalized(),
                    )),
                    Err(_) => bail!(BLOCK_NOT_FOUND),
                }
            } else {
                bail!(BLOCK_NOT_FOUND);
            }
        }
    }

    async fn get_slot_range(&self) -> Range<Slot> {
        let in_memory = self.inmemory_for_storage.get_slot_range().await;
        // if faithful is available we assume that we have all the blocks
        if self.faithful_rpc_client.is_some() {
            0..in_memory.end
        } else {
            let persistent_storage_range = self.persistent_block_storage.get_slot_range().await;
            persistent_storage_range.start..in_memory.end
        }
    }
}
