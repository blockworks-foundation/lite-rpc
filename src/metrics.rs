use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Metrics {
    pub txs_sent: usize,
    pub txs_confirmed: usize,
    pub txs_finalized: usize,
    pub txs_ps: usize,
    pub txs_confirmed_ps: usize,
    pub txs_finalized_ps: usize,
}
