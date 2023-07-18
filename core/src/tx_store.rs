use std::sync::Arc;

use dashmap::DashMap;
use solana_transaction_status::TransactionStatus;
/// Transaction Properties

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

pub type TxStore = Arc<DashMap<String, TxProps>>;

pub fn empty_tx_store() -> TxStore {
    Arc::new(DashMap::new())
}
