use std::sync::Arc;

use dashmap::DashMap;

use log::info;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::sync::RwLock;

use crate::workers::BlockInformation;

#[derive(Clone)]
pub struct BlockStore {
    blocks: Arc<DashMap<String, BlockInformation>>,
    latest_confirmed_blockhash: Arc<RwLock<String>>,
    latest_finalized_blockhash: Arc<RwLock<String>>,
}

impl BlockStore {
    pub async fn new(rpc_client: &RpcClient) -> anyhow::Result<Self> {
        let (confirmed_blockhash, confirmed_block) =
            Self::fetch_latest(rpc_client, CommitmentConfig::confirmed()).await?;
        let (finalized_blockhash, finalized_block) =
            Self::fetch_latest(rpc_client, CommitmentConfig::finalized()).await?;

        Ok(Self {
            latest_confirmed_blockhash: Arc::new(RwLock::new(confirmed_blockhash.clone())),
            latest_finalized_blockhash: Arc::new(RwLock::new(finalized_blockhash.clone())),
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
        let (latest_block_hash, block_height) = rpc_client
            .get_latest_blockhash_with_commitment(commitment_config)
            .await?;

        let latest_block_hash = latest_block_hash.to_string();
        let slot = rpc_client
            .get_slot_with_commitment(commitment_config)
            .await?;

        Ok((latest_block_hash, BlockInformation { slot, block_height }))
    }

    pub async fn get_block_info(&self, blockhash: &str) -> Option<BlockInformation> {
        let Some(info) = self.blocks.get(blockhash) else {
            return None;
        };

        Some(info.value().to_owned())
    }

    pub fn get_latest_blockhash(&self, commitment_config: CommitmentConfig) -> Arc<RwLock<String>> {
        if commitment_config.is_finalized() {
            self.latest_finalized_blockhash.clone()
        } else {
            self.latest_confirmed_blockhash.clone()
        }
    }

    pub async fn get_latest_block_info(
        &self,
        commitment_config: CommitmentConfig,
    ) -> (String, BlockInformation) {
        let blockhash = self
            .get_latest_blockhash(commitment_config)
            .read()
            .await
            .to_owned();

        let block_info = self
            .blocks
            .get(&blockhash)
            .expect("Race Condition: Latest block not in block store")
            .value()
            .to_owned();

        (blockhash, block_info)
    }

    pub async fn add_block(
        &self,
        blockhash: String,
        block_info: BlockInformation,
        commitment_config: CommitmentConfig,
    ) {
        info!("ab {blockhash} {block_info:?}");
        // Write to block store first in order to prevent
        // any race condition i.e prevent some one to
        // ask the map what it doesn't have rn
        self.blocks.insert(blockhash.clone(), block_info);
        *self.get_latest_blockhash(commitment_config).write().await = blockhash;
    }
}
