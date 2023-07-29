use std::sync::Arc;

use dashmap::DashMap;
use solana_transaction_status::TransactionStatus;

#[derive(Debug, Clone, Default)]
pub struct TxMeta {
    pub status: Option<TransactionStatus>,
    pub last_valid_blockheight: u64,
}

pub type TxStore = Arc<DashMap<String, TxMeta>>;
