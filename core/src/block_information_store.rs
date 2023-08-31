use dashmap::DashMap;
use log::info;

use solana_sdk::{
    clock::MAX_RECENT_BLOCKHASHES, commitment_config::CommitmentConfig, slot_history::Slot,
};
use std::sync::Arc;
use tokio::{sync::RwLock, time::Instant};

use crate::structures::processed_block::ProcessedBlock;

#[derive(Clone, Debug)]
pub struct BlockInformation {
    pub slot: u64,
    pub block_height: u64,
    pub last_valid_blockheight: u64,
    pub cleanup_slot: Slot,
    pub blockhash: String,
}

impl BlockInformation {
    pub fn from_block(block: &ProcessedBlock) -> Self {
        BlockInformation {
            slot: block.slot,
            block_height: block.block_height,
            last_valid_blockheight: block.block_height + MAX_RECENT_BLOCKHASHES as u64,
            cleanup_slot: block.block_height + 1000,
            blockhash: block.blockhash.clone(),
        }
    }
}

#[derive(Clone)]
pub struct BlockInformationStore {
    blocks: Arc<DashMap<String, BlockInformation>>,
    latest_processed_block: Arc<RwLock<BlockInformation>>,
    latest_confirmed_block: Arc<RwLock<BlockInformation>>,
    latest_finalized_block: Arc<RwLock<BlockInformation>>,
    last_add_block_metric: Arc<RwLock<Instant>>,
}

impl BlockInformationStore {
    pub fn new(latest_finalized_block: BlockInformation) -> Self {
        let blocks = Arc::new(DashMap::new());

        blocks.insert(
            latest_finalized_block.blockhash.clone(),
            latest_finalized_block.clone(),
        );

        Self {
            latest_processed_block: Arc::new(RwLock::new(latest_finalized_block.clone())),
            latest_confirmed_block: Arc::new(RwLock::new(latest_finalized_block.clone())),
            latest_finalized_block: Arc::new(RwLock::new(latest_finalized_block)),
            blocks,
            last_add_block_metric: Arc::new(RwLock::new(Instant::now())),
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
        commitment_config: CommitmentConfig,
    ) -> Arc<RwLock<BlockInformation>> {
        if commitment_config.is_finalized() {
            self.latest_finalized_block.clone()
        } else if commitment_config.is_confirmed() {
            self.latest_confirmed_block.clone()
        } else {
            self.latest_processed_block.clone()
        }
    }

    pub async fn get_latest_blockhash(&self, commitment_config: CommitmentConfig) -> String {
        self.get_latest_block_arc(commitment_config)
            .read()
            .await
            .blockhash
            .clone()
    }

    pub async fn get_latest_block_info(
        &self,
        commitment_config: CommitmentConfig,
    ) -> BlockInformation {
        self.get_latest_block_arc(commitment_config)
            .read()
            .await
            .clone()
    }

    pub async fn get_latest_block(&self, commitment_config: CommitmentConfig) -> BlockInformation {
        self.get_latest_block_arc(commitment_config)
            .read()
            .await
            .clone()
    }

    pub async fn add_block(
        &self,
        block_info: BlockInformation,
        commitment_config: CommitmentConfig,
    ) {
        // create context for add block metric
        {
            let mut last_add_block_metric = self.last_add_block_metric.write().await;
            *last_add_block_metric = Instant::now();
        }

        // save slot copy to avoid borrow issues
        let slot = block_info.slot;

        // Write to block store first in order to prevent
        // any race condition i.e prevent some one to
        // ask the map what it doesn't have rn
        self.blocks
            .insert(block_info.blockhash.clone(), block_info.clone());

        // update latest block
        let latest_block = self.get_latest_block_arc(commitment_config);
        if slot > latest_block.read().await.slot {
            *latest_block.write().await = block_info;
        }
    }

    pub async fn clean(&self) {
        let finalized_block_information = self
            .get_latest_block_info(CommitmentConfig::finalized())
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
}
