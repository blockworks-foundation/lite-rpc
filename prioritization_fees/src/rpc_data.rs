use jsonrpsee::core::Serialize;
use solana_sdk::{clock::Slot, pubkey::Pubkey};
use std::{collections::HashMap, fmt::Display, sync::Arc};

#[derive(Clone, Serialize, Debug, Default)]
pub struct TxAggregateStats {
    pub total: u64,
    pub nonvote: u64,
}

#[derive(Clone, Serialize, Debug, Default)]
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

impl PrioFeesStats {
    pub fn get_percentile(&self, percentile: f32) -> Option<(u64, u64)> {
        let index_tx = self
            .by_tx_percentiles
            .iter()
            .enumerate()
            .find(|(_, v)| **v == percentile)
            .map(|(ix, _)| ix);
        let index_cu = self
            .by_cu_percentiles
            .iter()
            .enumerate()
            .find(|(_, v)| **v == percentile)
            .map(|(ix, _)| ix);
        if let Some(index_tx) = index_tx {
            if let Some(index_cu) = index_cu {
                return Some((self.by_tx[index_tx], self.by_cu[index_cu]));
            }
        }
        None
    }
}

#[derive(Clone, Serialize, Debug, Eq, PartialEq, Hash)]
pub struct FeePoint {
    // percentile
    pub percentile: u32,
    // value of fees in lamports
    pub fees: u64,
}

impl Display for FeePoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(p{}, {})", self.percentile, self.fees)
    }
}

#[derive(Clone, Debug)]
pub struct PrioFeesUpdateMessage {
    pub slot: Slot,
    pub priofees_stats: PrioFeesStats,
}

#[derive(Clone, Serialize, Debug)]
pub struct AccountPrioFeesStats {
    pub write_stats: PrioFeesStats,
    pub all_stats: PrioFeesStats,
}

#[derive(Clone)]
pub struct AccountPrioFeesUpdateMessage {
    pub slot: Slot,
    pub accounts_data: Arc<HashMap<Pubkey, AccountPrioFeesStats>>,
}
