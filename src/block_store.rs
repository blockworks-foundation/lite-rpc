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
    latest_confirmed_blockhash: Arc<RwLock<(String, BlockInformation)>>,
    latest_finalized_blockhash: Arc<RwLock<(String, BlockInformation)>>,
}

impl BlockStore {
    pub async fn new(rpc_client: &RpcClient) -> anyhow::Result<Self> {
        let (confirmed_blockhash, confirmed_block) =
            Self::fetch_latest(rpc_client, CommitmentConfig::confirmed()).await?;
        let (finalized_blockhash, finalized_block) =
            Self::fetch_latest(rpc_client, CommitmentConfig::finalized()).await?;

        Ok(Self {
            latest_confirmed_blockhash: Arc::new(RwLock::new((
                confirmed_blockhash.clone(),
                confirmed_block,
            ))),
            latest_finalized_blockhash: Arc::new(RwLock::new((
                finalized_blockhash.clone(),
                finalized_block,
            ))),
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

    fn get_latest_block_arc(
        &self,
        commitment_config: CommitmentConfig,
    ) -> Arc<RwLock<(String, BlockInformation)>> {
        if commitment_config.is_finalized() {
            self.latest_finalized_blockinfo.clone()
        } else {
            self.latest_confirmed_blockinfo.clone()
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
        block_info: BlockInformation,
        commitment_config: CommitmentConfig,
    ) {
        info!("ab {blockhash} {block_info:?}");
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
