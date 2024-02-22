use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::message::v0::MessageAddressTableLookup;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::{slot_history::Slot, transaction::TransactionError};
use solana_transaction_status::Reward;
use std::ops::Deref;
use std::sync::Arc;

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
    pub writable_accounts: Vec<Pubkey>,
    pub readable_accounts: Vec<Pubkey>,
    pub address_lookup_tables: Vec<MessageAddressTableLookup>,
}

#[derive(Debug, Clone)]
pub struct ProducedBlock {
    // Arc is required for channels
    inner: Arc<ProducedBlockInner>,
    pub commitment_config: CommitmentConfig,
}

impl ProducedBlock {
    pub fn new(inner: ProducedBlockInner, commitment_config: CommitmentConfig) -> Self {
        ProducedBlock {
            inner: Arc::new(inner),
            commitment_config,
        }
    }
}

impl Deref for ProducedBlock {
    type Target = ProducedBlockInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug)]
pub struct ProducedBlockInner {
    pub transactions: Vec<TransactionInfo>,
    pub leader_id: Option<String>,
    pub blockhash: String,
    pub block_height: u64,
    pub slot: Slot,
    pub parent_slot: Slot,
    pub block_time: u64,
    pub previous_blockhash: String,
    pub rewards: Option<Vec<Reward>>,
}

impl ProducedBlock {
    /// moving commitment level to confirmed
    pub fn to_confirmed_block(&self) -> Self {
        ProducedBlock {
            inner: self.inner.clone(),
            commitment_config: CommitmentConfig::confirmed(),
        }
    }

    /// moving commitment level to finalized
    pub fn to_finalized_block(&self) -> Self {
        ProducedBlock {
            inner: self.inner.clone(),
            commitment_config: CommitmentConfig::finalized(),
        }
    }
}

