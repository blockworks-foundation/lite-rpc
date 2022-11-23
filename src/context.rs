use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc, RwLock},
};

pub struct BlockInformation {
    pub block_hash: RwLock<String>,
    pub block_height: AtomicU64,
    pub slot: AtomicU64,
    pub confirmation_level: CommitmentLevel,
}

impl BlockInformation {
    pub fn new(rpc_client: Arc<RpcClient>, commitment: CommitmentLevel) -> Self {
        let slot = rpc_client
            .get_slot_with_commitment(CommitmentConfig { commitment })
            .unwrap();

        let (blockhash, blockheight) = rpc_client
            .get_latest_blockhash_with_commitment(CommitmentConfig { commitment })
            .unwrap();

        BlockInformation {
            block_hash: RwLock::new(blockhash.to_string()),
            block_height: AtomicU64::new(blockheight),
            slot: AtomicU64::new(slot),
            confirmation_level: commitment,
        }
    }
}

pub struct LiteRpcContext {
    pub signature_status: RwLock<HashMap<String, Option<CommitmentLevel>>>,
    pub finalized_block_info: BlockInformation,
    pub confirmed_block_info: BlockInformation,
}

impl LiteRpcContext {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        LiteRpcContext {
            signature_status: RwLock::new(HashMap::new()),
            confirmed_block_info: BlockInformation::new(
                rpc_client.clone(),
                CommitmentLevel::Confirmed,
            ),
            finalized_block_info: BlockInformation::new(rpc_client, CommitmentLevel::Finalized),
        }
    }
}
