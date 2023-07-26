use chrono::{DateTime, Utc};
use dashmap::DashMap;
use log::info;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};
use std::sync::Arc;
use tokio::{sync::RwLock, time::Instant};

#[derive(Debug, Clone, Copy)]
pub struct Block {
    hash: String,
    info: BlockMeta,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BlockMeta {
    pub slot: u64,
    pub block_height: u64,
    pub last_valid_blockheight: u64,
    pub cleanup_slot: Slot,
    pub processed_local_time: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub struct BlockStore {
    blocks: Arc<DashMap<String, BlockMeta>>,
    latest_block: Arc<DashMap<CommitmentLevel, Block>>,
}

impl BlockStore {
    pub fn new() -> Self {
        Self {
            blocks: Default::default(),
            latest_block: Default::default(),
        }
    }

    pub fn get_block_info(&self, blockhash: &str) -> Option<BlockMeta> {
        self.blocks
            .get(blockhash)
            .map(|info| info.value().to_owned())
    }

    fn get_latest_block(&self, commitment_config: &CommitmentConfig) -> (String, BlockMeta) {
        self.latest_block
            .get(&commitment_config.commitment)
            .expect("Blockstore is empty")
            .to_owned()
    }

    pub fn get_latest_blockhash(&self, commitment_config: &CommitmentConfig) -> String {
        self.get_latest_block(commitment_config).0
    }

    pub fn get_latest_block_info(&self, commitment_config: &CommitmentConfig) -> BlockMeta {
        self.get_latest_block(commitment_config).1
    }

    pub async fn add_block(
        &self,
        blockhash: String,
        mut block_info: BlockMeta,
        commitment_config: &CommitmentConfig,
    ) {
        // create context for add block metric
        {
            //            let mut last_add_block_metric = self.last_add_block_metric.write().await;
            //            *last_add_block_metric = Instant::now();
        }

        // override timestamp from previous value, so we always keep the earliest (processed) timestamp around
        if let Some(processed_block) = self.get_block_info(&blockhash.clone()) {
            block_info.processed_local_time = processed_block.processed_local_time;
        }

        // save slot copy to avoid borrow issues
        let slot = block_info.slot;

        // Write to block store first in order to prevent
        // any race condition i.e prevent some one to
        // ask the map what it doesn't have rn
        self.blocks.insert(blockhash.clone(), block_info);

        // update latest block
        let latest_block_slot = self.get_latest_block_info(&commitment_config).slot;
        if slot > latest_block_slot {
            self.latest_block.insert(
                commitment_config.commitment,
                (blockhash.clone(), self.get_block_info(&blockhash).unwrap()),
            );
        }
    }

    pub async fn clean(&self) {
        let finalized_block_information = self
            .get_latest_block_info(&CommitmentConfig::finalized())
            .await;

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
