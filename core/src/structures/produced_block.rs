use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::{slot_history::Slot, transaction::TransactionError};
use solana_transaction_status::Reward;

#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub signature: String,
    pub is_vote: bool,
    pub err: Option<TransactionError>,
    pub cu_requested: Option<u32>,
    pub prioritization_fees: Option<u64>,
    pub cu_consumed: Option<u64>,
    pub recent_blockhash: String,
    pub message: String,
}

// TODO try to remove Clone
#[derive(Debug, Clone)]
pub struct ProducedBlock {
    pub transactions: Vec<TransactionInfo>,
    pub leader_id: Option<String>,
    pub blockhash: String,
    pub block_height: u64,
    pub slot: Slot,
    pub parent_slot: Slot,
    pub block_time: u64,
    pub commitment_config: CommitmentConfig,
    pub previous_blockhash: String,
    pub rewards: Option<Vec<Reward>>,
    pub bloat: [u8; BLOAT_SIZE],
}
pub const BLOAT_SIZE: usize = 100_000;

impl ProducedBlock {
    /// moving commitment level to finalized
    pub fn to_finalized_block(&self) -> Box<ProducedBlock> {
        Box::new(ProducedBlock {
            commitment_config: CommitmentConfig::finalized(),
            ..self.clone()
        })
    }

    /// moving commitment level to confirmed
    pub fn to_confirmed_block(&self) -> Box<ProducedBlock> {
        Box::new(ProducedBlock {
            commitment_config: CommitmentConfig::confirmed(),
            ..self.clone()
        })
    }
}
