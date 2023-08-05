use chrono::{DateTime, Utc};
use dashmap::DashMap;
use log::info;
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Block {
    pub blockhash: String,
    pub meta: BlockMeta,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BlockMeta {
    pub slot: u64,
    pub block_height: u64,
    pub last_valid_blockheight: u64,
    pub cleanup_slot: Slot,
    pub processed_local_time: Option<DateTime<Utc>>,
}

#[derive(Default, Clone, Debug)]
pub struct BlockStore {
    blocks: Arc<DashMap<String, BlockMeta>>,
    latest_block: Arc<DashMap<CommitmentConfig, Block>>,
}

impl BlockStore {
    pub fn get_block_info(&self, blockhash: &str) -> Option<BlockMeta> {
        self.blocks
            .get(blockhash)
            .map(|info| info.value().to_owned())
    }

    pub fn get_latest_block(&self, commitment_config: &CommitmentConfig) -> Block {
        self.latest_block
            .get(commitment_config)
            .expect("Blockstore is empty")
            .to_owned()
    }

    pub fn get_latest_blockhash(&self, commitment_config: &CommitmentConfig) -> String {
        self.get_latest_block(commitment_config).blockhash
    }

    pub fn get_latest_block_meta(&self, commitment_config: &CommitmentConfig) -> BlockMeta {
        self.get_latest_block(commitment_config).meta
    }

    pub fn cotains_block(&self, blockhash: &str) -> bool {
        self.blocks.contains_key(blockhash)
    }

    pub async fn add_block(
        &self,
        blockhash: String,
        mut meta: BlockMeta,
        commitment_config: CommitmentConfig,
    ) {
        // override timestamp from previous value, so we always keep the earliest (processed) timestamp around
        if let Some(processed_block) = self.get_block_info(&blockhash) {
            meta.processed_local_time = processed_block.processed_local_time;
        }

        // save slot copy to avoid borrow issues
        let slot = meta.slot;

        // Write to block store first in order to prevent
        // any race condition i.e prevent some one to
        // ask the map what it doesn't have rn
        self.blocks.insert(blockhash.clone(), meta);

        // update latest block
        let latest_block_slot = self.get_latest_block_meta(&commitment_config).slot;
        if slot > latest_block_slot {
            self.latest_block
                .insert(commitment_config, Block { blockhash, meta });
        }
    }

    pub async fn clean(&self) {
        let finalized_block_information =
            self.get_latest_block_meta(&CommitmentConfig::finalized());

        let before_length = self.blocks.len();
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
