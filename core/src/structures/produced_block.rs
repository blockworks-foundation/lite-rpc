use itertools::Itertools;
use log::{debug, info, trace};
use solana_lite_rpc_util::statistics::percentiles::calculate_percentiles;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::message::v0::MessageAddressTableLookup;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::{slot_history::Slot, transaction::TransactionError};
use solana_transaction_status::Reward;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::debug_span;

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

        ARC_PRODUCED_BLOCK
            .lock()
            .unwrap()
            .push((weak, Instant::now()));

        inspect();

        ProducedBlock {
            inner: arc,
            commitment_config,
        }
    }
}

fn inspect() {
    let _span = debug_span!("produced_block_inspect_refs").entered();
    let mut references = ARC_PRODUCED_BLOCK.lock().unwrap();

    let mut live = 0;
    let mut freed = 0;
    for r in references.iter() {
        trace!(
            "- {} refs, created at {:?}",
            r.0.strong_count(),
            r.1.elapsed()
        );
        if r.0.strong_count() == 0 {
            freed += 1;
        } else {
            live += 1;
        }
    }

    if freed >= 100 {
        references.retain(|r| r.0.strong_count() > 0);
    }

    let dist = references
        .iter()
        .filter(|r| r.0.strong_count() > 0)
        .map(|r| r.1.elapsed().as_secs_f64() * 1000.0)
        .sorted_by(|a, b| a.partial_cmp(b).unwrap())
        .collect::<Vec<f64>>();

    let percentiles = calculate_percentiles(&dist);
    trace!(
        "debug refs helt on ProducedBlock Arc - percentiles of time_ms: {}",
        percentiles
    );
    debug!(
        "refs helt on ProducedBlock: live: {}, freed: {}, p50={:.1}ms, p95={:.1}ms, max={:.1}ms",
        live,
        freed,
        percentiles.get_bucket_value(0.50).unwrap(),
        percentiles.get_bucket_value(0.95).unwrap(),
        percentiles.get_bucket_value(1.0).unwrap(),
    );
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
