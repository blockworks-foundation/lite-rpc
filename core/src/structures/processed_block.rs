use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};

use super::transaction_info::TransactionInfo;

#[derive(Default, Debug, Clone)]
pub struct ProcessedBlock {
    pub txs: Vec<TransactionInfo>,
    pub leader_id: Option<String>,
    pub blockhash: String,
    pub block_height: u64,
    pub slot: Slot,
    pub parent_slot: Slot,
    pub block_time: u64,
    pub commitment_config: CommitmentConfig,
}

pub enum BlockProcessorError {
    Incomplete,
}
