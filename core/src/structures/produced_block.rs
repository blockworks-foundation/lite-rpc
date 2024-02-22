use std::collections::HashMap;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::message::v0::MessageAddressTableLookup;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::{slot_history::Slot, transaction::TransactionError};
use solana_transaction_status::Reward;
use std::fmt::Debug;
use std::ops::Deref;
use std::rc::Weak;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use log::info;

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



lazy_static::lazy_static! {
    static ref ARC_PRODUCED_BLOCK: Mutex<Vec<(std::sync::Weak<ProducedBlockInner>, Instant)>> = Mutex::new(Vec::with_capacity(1000));
}


#[derive(Clone)]
pub struct ProducedBlock {
    // Arc is required for channels
    inner: Arc<ProducedBlockInner>,
    pub commitment_config: CommitmentConfig,
}

impl ProducedBlock {
    pub fn new(inner: ProducedBlockInner, commitment_config: CommitmentConfig) -> Self {
        let arc = Arc::new(inner);
        let weak: std::sync::Weak<ProducedBlockInner> = Arc::downgrade(&arc);

        ARC_PRODUCED_BLOCK.lock().unwrap()
            .push((weak, Instant::now()));

        inspect();

        ProducedBlock {
            inner: arc,
            commitment_config,
        }
    }
}

fn inspect() {
    let references = ARC_PRODUCED_BLOCK.lock().unwrap();
    info!("references: {}", references.len());
    for r in references.iter() {
        info!("- #{} {:?}", r.0.strong_count(), r.1.elapsed());
    }

}

/// # Example
/// ```text
/// ProducedBlock { slot: 254169151, commitment_config: processed, blockhash: BULfZwLswkDbHhTrHGDASUtmNAG8gk6TV2njnobjYLyd, transactions_count: 806 }
/// ```
impl Debug for ProducedBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProducedBlock {{ slot: {}, commitment_config: {}, blockhash: {}, transactions_count: {} }}",
               self.slot, self.commitment_config.commitment, self.blockhash, self.transactions.len())
    }
}

impl Deref for ProducedBlock {
    type Target = ProducedBlockInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

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
