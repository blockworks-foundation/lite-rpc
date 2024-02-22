use std::collections::HashMap;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::message::v0::MessageAddressTableLookup;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::{slot_history::Slot, transaction::TransactionError};
use solana_transaction_status::Reward;
use std::fmt::Debug;
use std::ops::Deref;
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
    // TODO use slab
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
    let mut freed = 0;
    let mut age_100ms = 0;
    let mut age_500ms = 0;
    let mut age_5s = 0;
    let mut age_60s = 0;
    for r in references.iter() {
        info!("- {} refs, created at {:?}", r.0.strong_count(), r.1.elapsed());
        if r.0.strong_count() == 0 {
            freed += 1;
            continue;
        }
        let age_ms = r.1.elapsed().as_millis();
        if age_ms < 100 {
            age_100ms += 1;
            continue;
        }
        if age_ms < 500 {
            age_500ms += 1;
            continue;
        }
        if age_ms < 5_000 {
            age_5s += 1;
            continue;
        }
        if age_ms < 60_000 {
            age_60s += 1;
            continue;
        }
    }
    let live = references.len() - freed;
    info!("live: {}, freed: {}, <100ms: {}, <500ms: {}, <5s: {}, <60s: {}", live, freed, age_100ms, age_500ms, age_5s, age_60s);

    let hist = histogram(&references.iter().map(|r| r.1.elapsed().as_secs_f64() * 1000.0).collect::<Vec<f64>>(), 10);
    info!("histogram: {:?}", hist);

}

pub fn histogram(values: &[f64], bins: usize) -> Vec<(f64, usize)> {
    assert!(bins >= 2);
    let mut bucket: Vec<usize> = vec![0; bins];

    let mut min = std::f64::MAX;
    let mut max = std::f64::MIN;
    for val in values {
        min = min.min(*val);
        max = max.max(*val);
    }
    let step = (max - min) / (bins - 1) as f64;

    for &v in values {
        let i = std::cmp::min(((v - min) / step).ceil() as usize, bins - 1);
        bucket[i] += 1;
    }

    bucket
        .into_iter()
        .enumerate()
        .map(|(i, v)| (min + step * i as f64, v))
        .collect()
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
