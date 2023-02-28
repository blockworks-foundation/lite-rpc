use std::sync::Arc;

use anyhow::Context;
use dashmap::DashMap;

use log::info;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::TransactionDetails;
use tokio::sync::RwLock;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug)]
pub struct BlockInformation {
    pub slot: u64,
    pub block_height: u64,
}

#[derive(Clone)]
pub struct BlockStore {
    blocks: Arc<DashMap<String, BlockInformation>>,
    latest_confirmed_block: Arc<RwLock<(String, BlockInformation)>>,
    latest_finalized_block: Arc<RwLock<(String, BlockInformation)>>,
    last_add_block_metric: Arc<RwLock<Instant>>,
}

impl BlockStore {
    pub async fn new(rpc_client: &RpcClient) -> anyhow::Result<Self> {
        let (confirmed_blockhash, confirmed_block) =
            Self::fetch_latest(rpc_client, CommitmentConfig::confirmed()).await?;
        let (finalized_blockhash, finalized_block) =
            Self::fetch_latest(rpc_client, CommitmentConfig::finalized()).await?;

        Ok(Self {
            latest_confirmed_block: Arc::new(RwLock::new((
                confirmed_blockhash.clone(),
                confirmed_block,
            ))),
            latest_finalized_block: Arc::new(RwLock::new((
                finalized_blockhash.clone(),
                finalized_block,
            ))),
            blocks: Arc::new({
                let map = DashMap::new();
                map.insert(confirmed_blockhash, confirmed_block);
                map.insert(finalized_blockhash, finalized_block);
                map
            }),
            last_add_block_metric: Arc::new(RwLock::new(Instant::now())),
        })
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

        Ok((latest_block_hash, BlockInformation { slot, block_height }))
    }

    pub async fn get_block_info(&self, blockhash: &str) -> Option<BlockInformation> {
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
        } else {
            self.latest_confirmed_block.clone()
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
        block_info: BlockInformation,
        commitment_config: CommitmentConfig,
    ) {
        // create context for add block metric
        {
            let mut last_add_block_metric = self.last_add_block_metric.write().await;

            info!(
                "{:?} {blockhash} with info {block_info:?}",
                last_add_block_metric.elapsed()
            );

            *last_add_block_metric = Instant::now();
        }

        // Write to block store first in order to prevent
        // any race condition i.e prevent some one to
        // ask the map what it doesn't have rn
        let slot = block_info.slot;
        self.blocks.insert(blockhash.clone(), block_info);

        let latest_block = self.get_latest_block_arc(commitment_config);
        if slot > latest_block.read().await.1.slot {
            *latest_block.write().await = (blockhash, block_info);
        }
    }
}

