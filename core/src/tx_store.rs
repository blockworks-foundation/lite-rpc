use std::sync::{atomic::Ordering, Arc};

use crate::AtomicSlot;
use dashmap::DashMap;
use solana_transaction_status::TransactionStatus;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct TxProps {
    pub status: Option<TransactionStatus>,
    pub last_valid_blockheight: u64,
}

impl TxProps {
    pub fn new(last_valid_blockheight: u64) -> Self {
        Self {
            status: Default::default(),
            last_valid_blockheight,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct TxStore {
    txs: Arc<DashMap<String, TxProps>>,
    current_slot: AtomicSlot,
}

impl TxStore {
    pub fn current_slot(&self) -> u64 {
        self.current_slot.load(Ordering::Relaxed)
    }
}
