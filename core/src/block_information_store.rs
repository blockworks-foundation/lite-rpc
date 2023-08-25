use chrono::{DateTime, Utc};
use dashmap::DashMap;
use log::info;

use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Debug, Default)]
pub struct BlockMeta {
    pub slot: u64,
    pub block_height: u64,
    pub last_valid_blockheight: u64,
    pub cleanup_slot: Slot,
    pub processed_local_time: Option<DateTime<Utc>>,
    pub blockhash: String,
}

#[derive(Default, Clone, Debug)]
pub struct BlockInformationStore {
    blocks: Arc<DashMap<String, BlockMeta>>,
    latest_confirmed_block: Arc<RwLock<Option<BlockMeta>>>,
    latest_finalized_block: Arc<RwLock<Option<BlockMeta>>>,
}

impl BlockInformationStore {
    pub fn get_block_info(&self, blockhash: &str) -> Option<BlockMeta> {
        self.blocks
            .get(blockhash)
            .map(|info| info.value().to_owned())
    }

    pub async fn get_latest_block(
        &self,
        commitment_config: &CommitmentConfig,
    ) -> Option<BlockMeta> {
        if commitment_config.is_confirmed() {
            self.latest_confirmed_block.read().await.to_owned()
        } else if commitment_config.is_finalized() {
            self.latest_finalized_block.read().await.to_owned()
        } else {
            None
        }
    }

    pub async fn insert_latest_block(
        &self,
        commitment_config: &CommitmentConfig,
        block: BlockMeta,
    ) {
        if commitment_config.is_confirmed() {
            *self.latest_confirmed_block.write().await = Some(block);
        } else if commitment_config.is_finalized() {
            *self.latest_finalized_block.write().await = Some(block);
        } else {
            panic!("Attempted to insert latest block with invalid commitment level");
        }
    }

    pub async fn get_latest_blockhash(
        &self,
        commitment_config: &CommitmentConfig,
    ) -> Option<String> {
        self.get_latest_block(commitment_config)
            .await
            .map(|block| block.blockhash)
    }

    pub fn cotains_block(&self, blockhash: &str) -> bool {
        self.blocks.contains_key(blockhash)
    }

    pub async fn add_block(&self, mut meta: BlockMeta, commitment_config: CommitmentConfig) {
        // override timestamp from previous value, so we always keep the earliest (processed) timestamp around
        if let Some(processed_block) = self.get_block_info(&meta.blockhash) {
            meta.processed_local_time = processed_block.processed_local_time;
        }

        // save slot copy to avoid borrow issues
        let slot = meta.slot;

        // Write to block store first in order to prevent
        // any race condition i.e prevent some one to
        // ask the map what it doesn't have rn
        self.blocks.insert(meta.blockhash.clone(), meta.clone());

        // update latest block
        if let Some(latest_block_slot) = self.get_latest_block(&commitment_config).await {
            if slot < latest_block_slot.slot {
                return;
            }
        }

        self.insert_latest_block(&commitment_config, meta).await;
    }

    pub async fn clean(&self) {
        let before_length = self.blocks.len();

        if before_length == 0 {
            return;
        }

        let Some(finalized_block_information) =
            self.get_latest_block(&CommitmentConfig::finalized()).await else {
                return ;
            };

        self.blocks
            .retain(|_, v| v.last_valid_blockheight >= finalized_block_information.block_height);

        let cleaned = before_length.saturating_sub(self.number_of_blocks_in_store());

        info!("Cleaned {cleaned} block info");
    }

    #[inline]
    pub fn number_of_blocks_in_store(&self) -> usize {
        self.blocks.len()
    }
}
