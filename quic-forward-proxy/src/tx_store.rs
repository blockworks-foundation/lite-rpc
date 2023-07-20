use std::sync::Arc;

use dashmap::DashMap;
use solana_transaction_status::TransactionStatus;
use tokio::time::Instant;
/// Transaction Properties

pub struct TxProps {
    pub status: Option<TransactionStatus>,
    /// Time at which transaction was forwarded
    pub sent_at: Instant,
}

impl Default for TxProps {
    fn default() -> Self {
        Self {
            status: Default::default(),
            sent_at: Instant::now(),
        }
    }
}

pub type TxStore = Arc<DashMap<String, TxProps>>;

pub fn empty_tx_store() -> TxStore {
    Arc::new(DashMap::new())
}
