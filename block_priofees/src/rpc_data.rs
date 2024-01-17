use jsonrpsee::core::Serialize;

// used as RPC DTO
#[derive(Clone, Serialize, Debug)]
pub struct PrioFeesStats {
    pub p_min: u64,
    pub p_median: u64,
    pub p_75: u64,
    pub p_90: u64,
    pub p_max: u64,
}
