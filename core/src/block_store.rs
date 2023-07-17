use anyhow::Context;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use log::info;
use serde_json::json;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::RpcBlockConfig,
    rpc_request::RpcRequest,
    rpc_response::{Response, RpcBlockhash},
};
use solana_sdk::{
    clock::MAX_RECENT_BLOCKHASHES, commitment_config::CommitmentConfig, slot_history::Slot,
};
use solana_transaction_status::TransactionDetails;
use std::sync::Arc;
use tokio::{sync::RwLock, time::Instant};

#[derive(Clone, Copy, Debug)]
pub struct BlockInformation {
    pub slot: u64,
    pub block_height: u64,
    pub last_valid_blockheight: u64,
    pub cleanup_slot: Slot,
    pub processed_local_time: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct BlockStore {
    blocks: Arc<DashMap<String, BlockInformation>>,
    latest_processed_block: Arc<RwLock<(String, BlockInformation)>>,
    latest_confirmed_block: Arc<RwLock<(String, BlockInformation)>>,
    latest_finalized_block: Arc<RwLock<(String, BlockInformation)>>,
    last_add_block_metric: Arc<RwLock<Instant>>,
}

impl BlockStore {
    pub async fn new(rpc_client: &RpcClient) -> anyhow::Result<Self> {
        let blocks = Arc::new(DashMap::new());

        // fetch in order of least recency so the blockstore is as up to date as it can be on boot
        let (finalized_blockhash, finalized_block) =
            Self::fetch_latest(rpc_client, CommitmentConfig::finalized()).await?;
        let (confirmed_blockhash, confirmed_block) =
            Self::fetch_latest(rpc_client, CommitmentConfig::confirmed()).await?;
        let (processed_blockhash, processed_block) =
            Self::poll_latest(rpc_client, CommitmentConfig::processed()).await?;

        blocks.insert(processed_blockhash.clone(), processed_block);
        blocks.insert(confirmed_blockhash.clone(), confirmed_block);
        blocks.insert(finalized_blockhash.clone(), finalized_block);

        Ok(Self {
            latest_processed_block: Arc::new(RwLock::new((processed_blockhash, processed_block))),
            latest_confirmed_block: Arc::new(RwLock::new((confirmed_blockhash, confirmed_block))),
            latest_finalized_block: Arc::new(RwLock::new((finalized_blockhash, finalized_block))),
            blocks,
            last_add_block_metric: Arc::new(RwLock::new(Instant::now())),
        })
    }

    pub async fn poll_latest(
        rpc_client: &RpcClient,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<(String, BlockInformation)> {
        let response = rpc_client
            .send::<Response<RpcBlockhash>>(
                RpcRequest::GetLatestBlockhash,
                json!([commitment_config]),
            )
            .await?;

        let processed_blockhash = response.value.blockhash;
        let processed_block = BlockInformation {
            slot: response.context.slot,
            last_valid_blockheight: response.value.last_valid_block_height,
            block_height: response
                .value
                .last_valid_block_height
                .saturating_sub(MAX_RECENT_BLOCKHASHES as u64),
            processed_local_time: Some(Utc::now()),
            cleanup_slot: response.value.last_valid_block_height + 700, // cleanup after 1000 slots
        };

        Ok((processed_blockhash, processed_block))
    }

    pub async fn fetch_latest(
        rpc_client: &RpcClient,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<(String, BlockInformation)> {
        let slot = rpc_client
            .get_slot_with_commitment(commitment_config)
            .await?;

        let block = rpc_client
            .get_block_with_config(
                slot,
                RpcBlockConfig {
                    encoding: None,
                    transaction_details: Some(TransactionDetails::None),
                    rewards: None,
                    commitment: Some(commitment_config),
                    max_supported_transaction_version: Some(0),
                },
            )
            .await?;

        let latest_block_hash = block.blockhash;
        let block_height = block
            .block_height
            .context("Couldn't get block height of latest block for block store")?;

        Ok((
            latest_block_hash,
            BlockInformation {
                slot,
                block_height,
                last_valid_blockheight: block_height + MAX_RECENT_BLOCKHASHES as u64,
                cleanup_slot: block_height + 1000,
                processed_local_time: None,
            },
        ))
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
    ) -> Arc<RwLock<(String, BlockInformation)>> {
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
            .0
            .clone()
    }

    pub async fn get_latest_block_info(
        &self,
        commitment_config: CommitmentConfig,
    ) -> BlockInformation {
        self.get_latest_block_arc(commitment_config).read().await.1
    }

    pub async fn get_latest_block(
        &self,
        commitment_config: CommitmentConfig,
    ) -> (String, BlockInformation) {
        self.get_latest_block_arc(commitment_config)
            .read()
            .await
            .clone()
    }

    pub async fn add_block(
        &self,
        blockhash: String,
        mut block_info: BlockInformation,
        commitment_config: CommitmentConfig,
    ) {
        // create context for add block metric
        {
            let mut last_add_block_metric = self.last_add_block_metric.write().await;
            *last_add_block_metric = Instant::now();
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
        let latest_block = self.get_latest_block_arc(commitment_config);
        if slot > latest_block.read().await.1.slot {
            *latest_block.write().await = (blockhash, block_info);
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
