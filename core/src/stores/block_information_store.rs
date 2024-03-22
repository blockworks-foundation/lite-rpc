use dashmap::DashMap;
use log::info;

use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::{clock::MAX_RECENT_BLOCKHASHES, slot_history::Slot};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::structures::block_info::BlockInfo;
use crate::structures::produced_block::ProducedBlock;
use solana_sdk::hash::Hash;

#[derive(Clone, Debug)]
pub struct BlockInformation {
    pub slot: u64,
    pub block_height: u64,
    pub last_valid_blockheight: u64,
    pub cleanup_slot: Slot,
    pub blockhash: Hash,
    pub commitment_config: CommitmentConfig,
    pub block_time: u64,
}

impl BlockInformation {
    pub fn from_block(block: &ProducedBlock) -> Self {
        BlockInformation {
            slot: block.slot,
            block_height: block.block_height,
            last_valid_blockheight: block.block_height + MAX_RECENT_BLOCKHASHES as u64,
            cleanup_slot: block.block_height + 1000,
            blockhash: block.blockhash,
            commitment_config: block.commitment_config,
            block_time: block.block_time,
        }
    }
    pub fn from_block_info(block_info: &BlockInfo) -> Self {
        BlockInformation {
            slot: block_info.slot,
            block_height: block_info.block_height,
            last_valid_blockheight: block_info.block_height + MAX_RECENT_BLOCKHASHES as u64,
            cleanup_slot: block_info.block_height + 1000,
            blockhash: block_info.blockhash,
            commitment_config: block_info.commitment_config,
            block_time: block_info.block_time,
        }
    }
}

/// - Block Information Store
/// This structure will store block information till where finalized block is still in range of last valid block hash.
/// So 300 blocks for last valid blockhash and 32 slots between confirmed and finalized.
/// So it should not store more than 400 blocks information.
#[derive(Clone)]
pub struct BlockInformationStore {
    // maps Block Hash -> Block information
    blocks: Arc<DashMap<Hash, BlockInformation>>,
    last_blockheight: Arc<AtomicU64>,
    latest_confirmed_block: Arc<RwLock<BlockInformation>>,
    latest_finalized_block: Arc<RwLock<BlockInformation>>,
}

impl BlockInformationStore {
    pub fn new(latest_finalized_block: BlockInformation) -> Self {
        let blocks = Arc::new(DashMap::new());

        blocks.insert(
            latest_finalized_block.blockhash,
            latest_finalized_block.clone(),
        );

        Self {
            last_blockheight: Arc::new(AtomicU64::new(latest_finalized_block.block_height)),
            latest_confirmed_block: Arc::new(RwLock::new(latest_finalized_block.clone())),
            latest_finalized_block: Arc::new(RwLock::new(latest_finalized_block)),
            blocks,
        }
    }

    pub fn get_block_info(&self, blockhash: &Hash) -> Option<BlockInformation> {
        let info = self.blocks.get(blockhash)?;

        Some(info.value().to_owned())
    }

    fn get_latest_block_arc(
        &self,
        commitment_config: CommitmentConfig,
    ) -> Arc<RwLock<BlockInformation>> {
        if commitment_config.is_finalized() {
            self.latest_finalized_block.clone()
        } else {
            self.latest_confirmed_block.clone()
        }
    }

    pub async fn get_latest_blockhash(&self, commitment_config: CommitmentConfig) -> Hash {
        self.get_latest_block_arc(commitment_config)
            .read()
            .await
            .blockhash
    }

    pub async fn get_latest_block_information(
        &self,
        commitment_config: CommitmentConfig,
    ) -> BlockInformation {
        self.get_latest_block_arc(commitment_config)
            .read()
            .await
            .clone()
    }

    pub async fn add_block(&self, block_info: BlockInformation) -> bool {
        // save slot copy to avoid borrow issues
        let slot = block_info.slot;
        let commitment_config = block_info.commitment_config;
        if self
            .last_blockheight
            .load(std::sync::atomic::Ordering::Relaxed)
            < block_info.block_height
        {
            // update last seen blockheight
            self.last_blockheight.store(
                block_info.block_height,
                std::sync::atomic::Ordering::Relaxed,
            );
        }

        // update latest block
        {
            let latest_block = self.get_latest_block_arc(commitment_config);
            if slot > latest_block.read().await.slot {
                *latest_block.write().await = block_info.clone();
            }
        }

        match self.blocks.entry(block_info.blockhash) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let should_update = match entry.get().commitment_config.commitment {
                    CommitmentLevel::Finalized => false, // should never update blocks of finalized commitment
                    CommitmentLevel::Confirmed => {
                        commitment_config == CommitmentConfig::finalized()
                    } // should only updated confirmed with finalized block
                    _ => {
                        commitment_config == CommitmentConfig::confirmed()
                            || commitment_config == CommitmentConfig::finalized()
                    }
                };
                if should_update {
                    entry.replace_entry(block_info);
                }
                should_update
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(block_info);
                true
            }
        }
    }

    pub async fn clean(&self) {
        let finalized_block_information = self
            .get_latest_block_information(CommitmentConfig::finalized())
            .await;
        let before_length = self.blocks.len();
        self.blocks
            .retain(|_, v| v.last_valid_blockheight >= finalized_block_information.block_height);

        info!(
            "Cleaned {} block info",
            before_length.saturating_sub(self.blocks.len())
        );
    }

    pub fn number_of_blocks_in_store(&self) -> usize {
        self.blocks.len()
    }

    pub async fn is_blockhash_valid(
        &self,
        blockhash: &Hash,
        commitment_config: CommitmentConfig,
    ) -> (bool, Slot) {
        let latest_block = self.get_latest_block_information(commitment_config).await;
        match self.blocks.get(blockhash) {
            Some(block_information) => (
                latest_block.block_height <= block_information.last_valid_blockheight,
                latest_block.slot,
            ),
            None => (false, latest_block.slot),
        }
    }

    pub fn get_block_info_by_slot(&self, slot: u64) -> Option<BlockInformation> {
        self.blocks.iter().find_map(|iter| {
            // should be fine for now, may be could be optimized in future if necessary
            if iter.slot == slot {
                Some(iter.value().clone())
            } else {
                None
            }
        })
    }

    pub fn get_last_blockheight(&self) -> u64 {
        self.last_blockheight
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}
