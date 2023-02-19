use std::sync::Arc;

use anyhow::Context;
use dashmap::DashMap;

use solana_client::rpc_config::RpcBlockConfig;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::TransactionDetails;
use tokio::sync::RwLock;

use crate::workers::BlockInformation;

#[derive(Clone)]
pub struct BlockStore {
    blocks: Arc<DashMap<String, BlockInformation>>,
    latest_confirmed_blockinfo: Arc<RwLock<BlockInformation>>,
    latest_finalized_blockinfo: Arc<RwLock<BlockInformation>>,
}

impl BlockStore {
    pub async fn new(rpc_client: &RpcClient) -> anyhow::Result<Self> {
        let (confirmed_blockhash, confirmed_block) =
            Self::fetch_latest(rpc_client, CommitmentConfig::confirmed()).await?;
        let (finalized_blockhash, finalized_block) =
            Self::fetch_latest(rpc_client, CommitmentConfig::finalized()).await?;

        Ok(Self {
            latest_confirmed_blockinfo: Arc::new(RwLock::new(confirmed_block.clone())),
            latest_finalized_blockinfo: Arc::new(RwLock::new(finalized_block.clone())),
            blocks: Arc::new({
                let map = DashMap::new();
                map.insert(confirmed_blockhash, confirmed_block);
                map.insert(finalized_blockhash, finalized_block);
                map
            }),
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

        Ok((
            latest_block_hash.clone(),
            BlockInformation {
                slot,
                block_height,
                blockhash: latest_block_hash,
            },
        ))
    }

    pub async fn get_block_info(&self, blockhash: &str) -> Option<BlockInformation> {
        let Some(info) = self.blocks.get(blockhash) else {
            return None;
        };

        Some(info.value().to_owned())
    }

    // private
    fn get_latest_blockinfo_lock(
        &self,
        commitment_config: CommitmentConfig,
    ) -> Arc<RwLock<BlockInformation>> {
        if commitment_config.is_finalized() {
            self.latest_finalized_blockinfo.clone()
        } else {
            self.latest_confirmed_blockinfo.clone()
        }
    }

    pub async fn get_latest_block_info(
        &self,
        commitment_config: CommitmentConfig,
    ) -> BlockInformation {
        let block_info = self
            .get_latest_blockinfo_lock(commitment_config)
            .read()
            .await
            .clone();

        block_info
    }

    pub async fn add_block(
        &self,
        block_info: BlockInformation,
        commitment_config: CommitmentConfig,
    ) {
        let blockhash = block_info.blockhash.clone();
        // Write to block store first in order to prevent
        // any race condition i.e prevent some one to
        // ask the map what it doesn't have rn
        self.blocks.insert(blockhash, block_info.clone());
        let last_recent_block = self.get_latest_block_info(commitment_config).await;

        if last_recent_block.slot < block_info.slot {
            *self
                .get_latest_blockinfo_lock(commitment_config)
                .write()
                .await = block_info;
        }
    }
}
