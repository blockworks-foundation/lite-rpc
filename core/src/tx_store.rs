use std::sync::{atomic::Ordering, Arc};

use dashmap::DashMap;
use solana_transaction_status::TransactionStatus;

#[derive(Debug, Clone, Default)]
pub struct TxMeta {
    pub status: Option<TransactionStatus>,
    pub last_valid_blockheight: u64,
}

#[derive(Default, Debug, Clone)]
pub struct TxStore {
    txs: Arc<DashMap<String, TxMeta>>,
}
