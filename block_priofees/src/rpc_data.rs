use jsonrpsee::core::Serialize;
use solana_sdk::clock::Slot;
use std::collections::HashMap;
use std::fmt::Display;

#[derive(Clone, Serialize, Debug)]
pub struct PrioFeesStats {
    pub fees_by_tx: Vec<u64>,
    pub percentiles_by_tx: Vec<f32>,
    pub fees_by_cu: Vec<u64>,
    pub percentiles_by_cu: Vec<f32>,
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
