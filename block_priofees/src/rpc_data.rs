use jsonrpsee::core::Serialize;
use solana_sdk::clock::Slot;
use std::fmt::Display;

#[derive(Clone, Serialize, Debug)]
pub struct TxAggregateStats {
    pub total: u64,
    pub nonvote: u64,
}

#[derive(Clone, Serialize, Debug)]
pub struct PrioFeesStats {
    // the arrays are same size and ordered monotonically
    pub by_tx: Vec<u64>,
    pub by_tx_percentiles: Vec<f32>,

    // the arrays are same size and ordered monotonically
    pub by_cu: Vec<u64>,
    pub by_cu_percentiles: Vec<f32>,

    // per block stats
    pub tx_count: TxAggregateStats,
    pub cu_consumed: TxAggregateStats,
}

#[derive(Clone, Serialize, Debug, Eq, PartialEq, Hash)]
pub struct FeePoint {
    // percentile
    pub p: u32,
    // value of fees in lamports
    pub v: u64,
}

impl Display for FeePoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(p{}, {})", self.p, self.v)
    }
}

#[derive(Clone, Debug)]
pub struct PrioFeesUpdateMessage {
    pub slot: Slot,
    pub priofees_stats: PrioFeesStats,
}
