use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Metrics {
    pub total_txs: usize,
    pub txs_confirmed: usize,
    pub txs_finalized: usize,
    pub in_secs: u64
}

