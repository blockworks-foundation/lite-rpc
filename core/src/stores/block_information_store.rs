use dashmap::DashMap;
use log::info;

use solana_sdk::{
    clock::MAX_RECENT_BLOCKHASHES,
    slot_history::Slot,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::commitment_utils::Commitment;

use crate::structures::produced_block::ProducedBlock;

#[derive(Clone, Debug)]
pub struct BlockInformation {
    pub slot: u64,
    pub block_height: u64,
    pub last_valid_blockheight: u64,
    pub cleanup_slot: Slot,
    pub blockhash: String,
    pub commitment_level: Commitment,
}

impl BlockInformation {
    pub fn from_block(block: &ProducedBlock) -> Self {
        BlockInformation {
            slot: block.slot,
            block_height: block.block_height,
            last_valid_blockheight: block.block_height + MAX_RECENT_BLOCKHASHES as u64,
            cleanup_slot: block.block_height + 1000,
            blockhash: block.blockhash.clone(),
            commitment_level: block.commitment_level,
        }
    }
}

#[derive(Clone)]
pub struct BlockInformationStore {
    blocks: Arc<DashMap<String, BlockInformation>>,
    latest_confirmed_block: Arc<RwLock<BlockInformation>>,
    latest_finalized_block: Arc<RwLock<BlockInformation>>,
}

impl BlockInformationStore {
    pub fn new(latest_finalized_block: BlockInformation) -> Self {
        let blocks = Arc::new(DashMap::new());

        blocks.insert(
            latest_finalized_block.blockhash.clone(),
            latest_finalized_block.clone(),
        );

        Self {
            latest_confirmed_block: Arc::new(RwLock::new(latest_finalized_block.clone())),
            latest_finalized_block: Arc::new(RwLock::new(latest_finalized_block)),
            blocks,
        }
    }

    pub fn get_block_info(&self, blockhash: &str) -> Option<BlockInformation> {
        let Some(info) = self.blocks.get(blockhash) else {
            return None;
        };

        Some(info.value().to_owned())
    }

    fn get_latest_block_arc(
        &self,
        commitment_level: Commitment,
    ) -> Arc<RwLock<BlockInformation>> {
        match commitment_level {
            Commitment::Processed | Commitment::Confirmed => self.latest_confirmed_block.clone(),
            Commitment::Finalized => self.latest_finalized_block.clone(),
        }
    }

    pub async fn get_latest_blockhash(&self, commitment_level: Commitment) -> String {
        self.get_latest_block_arc(commitment_level)
            .read()
            .await
            .blockhash
            .clone()
    }

    pub async fn get_latest_block_info(
        &self,
        commitment_level: Commitment,
    ) -> BlockInformation {
        self.get_latest_block_arc(commitment_level)
            .read()
            .await
            .clone()
    }

    pub async fn get_latest_block(&self, commitment_level: Commitment) -> BlockInformation {
        self.get_latest_block_arc(commitment_level)
            .read()
            .await
            .clone()
    }

    pub async fn add_block(&self, block_info: BlockInformation) -> bool {
        // save slot copy to avoid borrow issues
        let slot = block_info.slot;
        let commitment_level = block_info.commitment_level;
        // check if the block has already been added with higher commitment level
        match self.blocks.get_mut(&block_info.blockhash) {
            Some(mut prev_block_info) => {
                let should_update = match prev_block_info.commitment_level {
                    Commitment::Finalized => false, // should never update blocks of finalized commitment
                    Commitment::Confirmed => {
                        commitment_level == Commitment::Finalized
                    } // should only updated confirmed with finalized block
                    _ => {
                        commitment_level == Commitment::Confirmed
                            || commitment_level == Commitment::Finalized
                    }
                };
                if !should_update {
                    return false;
                }
                *prev_block_info = block_info.clone();
            }
            None => {
                self.blocks
                    .insert(block_info.blockhash.clone(), block_info.clone());
            }
        }

        // update latest block
        let latest_block = self.get_latest_block_arc(commitment_level);
        if slot > latest_block.read().await.slot {
            *latest_block.write().await = block_info;
        }
        true
    }

    pub async fn clean(&self) {
        let finalized_block_information = self
            .get_latest_block_info(Commitment::Finalized)
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
        blockhash: &String,
        commitment_level: Commitment,
    ) -> (bool, Slot) {
        let latest_block = self.get_latest_block(commitment_level).await;
        match self.blocks.get(blockhash) {
            Some(block_information) => (
                latest_block.block_height <= block_information.last_valid_blockheight,
                latest_block.slot,
            ),
            None => (false, latest_block.slot),
        }
    }
}
