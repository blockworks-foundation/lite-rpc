use std::sync::{Arc, RwLock};

use dashmap::DashMap;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;

use crate::workers::BlockInformation;

pub struct BlockStore {
    blocks: Arc<DashMap<String, BlockInformation>>,
    latest_block_hash: Arc<RwLock<String>>,
}

impl BlockStore {
    pub async fn new(
        rpc_client: &RpcClient,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<Self> {
        let (latest_block_hash, block_height) = rpc_client
            .get_latest_blockhash_with_commitment(commitment_config)
            .await?;

        let latest_block_hash = latest_block_hash.to_string();
        let slot = rpc_client
            .get_slot_with_commitment(commitment_config)
            .await?;

        Ok(Self {
            latest_block_hash: Arc::new(RwLock::new(latest_block_hash.clone())),
            blocks: Arc::new({
                let map = DashMap::new();
                map.insert(latest_block_hash, BlockInformation { slot, block_height });
                map
            }),
        })
    }
}
