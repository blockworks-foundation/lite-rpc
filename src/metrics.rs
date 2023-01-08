use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Metrics {
    pub total_txs_sent: usize,
    pub total_txs_confirmed: usize,
    pub total_txs_finalized: usize,
    pub txs_sent_in_one_sec: usize,
    pub txs_confirmed_in_one_sec: usize,
    pub txs_finalized_in_one_sec: usize
}

