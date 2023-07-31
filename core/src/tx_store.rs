use std::sync::Arc;

use dashmap::DashMap;
use solana_transaction_status::TransactionStatus;

#[derive(Debug, Clone, Default)]
pub struct TxMeta {
    pub status: Option<TransactionStatus>,
    pub last_valid_blockheight: u64,
}

#[derive(Default, Clone, Debug, derive_more::Deref)]
pub struct TxStore(Arc<DashMap<String, TxMeta>>);

impl TxStore {
    pub fn cleanup(&self, current_finalized_blochash: u64) {
        let length_before = self.len();
        self.retain(|_k, v| {
            let retain = v.last_valid_blockheight >= current_finalized_blochash;
            if !retain && v.status.is_none() {
                // TODO: TX_TIMED_OUT.inc();
            }
            retain
        });
        log::info!("Cleaned {} transactions", length_before - self.len());
    }
}
