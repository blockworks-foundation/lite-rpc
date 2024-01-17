use jsonrpsee::core::Serialize;
use solana_sdk::clock::Slot;
use std::collections::HashMap;

#[derive(Clone, Serialize, Debug)]
pub struct PrioFeesStats {
    pub p_min: u64,
    pub p_median: u64,
    pub p_75: u64,
    pub p_90: u64,
    pub p_max: u64,
    // (p0, 0), (p5, 100), (p10, 200), ..., (p95, 3000), (p100, 3000)
    pub dist_fee_by_index: Vec<(String, u64)>,

    pub p_median_cu: u64,
    pub p_75_cu: u64,
    pub p_90_cu: u64,
    pub p_95_cu: u64,
    // (p0, 0), (p5, 100), (p10, 200), ..., (p95, 3000), (p100, 3000)
    pub dist_fee_by_cu: Vec<(String, u64)>,
}

#[derive(Clone, Debug)]
pub struct PrioFeesUpdateMessage {
    pub slot: Slot,
    pub priofees_stats: PrioFeesStats,
}
