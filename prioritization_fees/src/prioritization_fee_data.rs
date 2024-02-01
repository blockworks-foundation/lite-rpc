use crate::{
    rpc_data::{PrioFeesStats, TxAggregateStats},
    stats_calculation::calculate_supp_percentiles,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct PrioFeesData {
    pub priority: u64,
    pub cu_consumed: u64,
}

impl From<(u64, u64)> for PrioFeesData {
    fn from(value: (u64, u64)) -> Self {
        Self {
            priority: value.0,
            cu_consumed: value.1,
        }
    }
}

#[derive(Default, Clone)]
pub struct BlockPrioData {
    pub transaction_data: Vec<PrioFeesData>,
    pub nb_non_vote_tx: u64,
    pub nb_total_tx: u64,
    pub non_vote_cu_consumed: u64,
    pub total_cu_consumed: u64,
}

impl BlockPrioData {
    pub fn calculate_stats(&self) -> PrioFeesStats {
        let priofees_percentiles = calculate_supp_percentiles(&self.transaction_data);

        PrioFeesStats {
            by_tx: priofees_percentiles.by_tx,
            by_tx_percentiles: priofees_percentiles.by_tx_percentiles,
            by_cu: priofees_percentiles.by_cu,
            by_cu_percentiles: priofees_percentiles.by_cu_percentiles,
            tx_count: TxAggregateStats {
                total: self.nb_total_tx,
                nonvote: self.nb_non_vote_tx,
            },
            cu_consumed: TxAggregateStats {
                total: self.total_cu_consumed,
                nonvote: self.non_vote_cu_consumed,
            },
        }
    }

    pub fn add(&self, rhs: &BlockPrioData) -> BlockPrioData {
        Self {
            transaction_data: [self.transaction_data.clone(), rhs.transaction_data.clone()]
                .concat(),
            nb_non_vote_tx: self.nb_non_vote_tx + rhs.nb_non_vote_tx,
            nb_total_tx: self.nb_total_tx + rhs.nb_total_tx,
            non_vote_cu_consumed: self.non_vote_cu_consumed + rhs.non_vote_cu_consumed,
            total_cu_consumed: self.total_cu_consumed + rhs.total_cu_consumed,
        }
    }
}
