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
    // p0, p5, p10, ..., p95, p100
    pub fine_percentiles: HashMap<String, u64>,

    pub p_median_cu: u64,
    pub p_75_cu: u64,
    pub p_90_cu: u64,
    pub p_95_cu: u64,
    // p0, p5, p10, ..., p95, p100
    pub fine_percentiles_cu: HashMap<String, u64>,
}

#[derive(Clone, Debug)]
pub struct PrioFeesUpdateMessage {
    pub slot: Slot,
    pub priofees_stats: PrioFeesStats,
}
