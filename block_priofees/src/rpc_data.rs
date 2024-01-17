use jsonrpsee::core::Serialize;
use solana_sdk::clock::Slot;
use std::collections::HashMap;
use std::fmt::Display;

#[derive(Clone, Serialize, Debug)]
pub struct PrioFeesStats {
    // (p0, 0), (p5, 100), (p10, 200), ..., (p95, 3000), (p100, 3000)
    pub fees_by_tx: Vec<FeePoint>,
    // (p0, 0), (p5, 100), (p10, 200), ..., (p95, 3000), (p100, 3000)
    pub fees_by_cu: Vec<FeePoint>,
}

#[derive(Clone, Serialize, Debug, Eq, PartialEq, Hash)]
pub struct FeePoint {
    // percentile
    pub p: String,
    // value of fees in lamports
    pub v: u64,
}

impl Display for FeePoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.p, self.v)
    }
}


#[derive(Clone, Debug)]
pub struct PrioFeesUpdateMessage {
    pub slot: Slot,
    pub priofees_stats: PrioFeesStats,
}
