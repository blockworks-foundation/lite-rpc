use jsonrpsee::core::Serialize;
use solana_sdk::clock::Slot;

#[derive(Clone, Serialize, Debug)]
pub struct PrioFeesStats {
    pub p_min: u64,
    pub p_median: u64,
    pub p_75: u64,
    pub p_90: u64,
    pub p_max: u64,
}


#[derive(Clone, Debug)]
pub struct PrioFeesUpdateMessage {
    pub slot: Slot,
    pub priofees_stats: PrioFeesStats,
}
