use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;

#[derive(Clone, Debug)]
pub struct BlockInfo {
    pub slot: u64,
    pub block_height: u64,
    pub blockhash: Hash,
    pub commitment_config: CommitmentConfig,
    pub block_time: u64,
}
