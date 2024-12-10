use crate::{prioritization_fee_data::PrioFeesData, rpc_data::FeePoint};
use itertools::Itertools;
use std::iter::zip;

// `quantile` function is the same as the median if q=50, the same as the minimum if q=0 and the same as the maximum if q=100.

pub fn calculate_supp_percentiles(
    // Vec(prioritization_fees, cu_consumed)
    prio_fees_in_block: &[PrioFeesData],
) -> Percentiles {
    let prio_fees_in_block = if prio_fees_in_block.is_empty() {
        // note: percentile for empty array is undefined
        vec![PrioFeesData::default()]
    } else {
        // sort by prioritization fees
        prio_fees_in_block
            .iter()
            .sorted_by_key(|data| data.priority)
            .cloned()
            .collect_vec()
    };

    // get stats by transaction
    let dist_fee_by_index: Vec<FeePoint> = (0..=100)
        .step_by(5)
        .map(|p| {
            let prio_fee = {
                let index = prio_fees_in_block.len() * p / 100;
                let cap_index = index.min(prio_fees_in_block.len().saturating_sub(1));
                prio_fees_in_block[cap_index].priority
            };
            FeePoint {
                percentile: p as u32,
                fees: prio_fee,
            }
        })
        .collect_vec();

    // get stats by CU
    let cu_sum: u64 = prio_fees_in_block.iter().map(|x| x.cu_consumed).sum();
    let mut agg: u64 = prio_fees_in_block[0].cu_consumed;
    let mut index = 0;
    let p_step = 5;

    let dist_fee_by_cu = (0..=100)
        .step_by(p_step)
        .map(|percentile| {
            while agg < (cu_sum * percentile) / 100 {
                index += 1;
                agg += prio_fees_in_block[index].cu_consumed;
            }
            let priority = prio_fees_in_block[index].priority;
            FeePoint {
                percentile: percentile as u32,
                fees: priority,
            }
        })
        .collect_vec();

    Percentiles {
        by_tx: dist_fee_by_index
            .iter()
            .map(|fee_point| fee_point.fees)
            .collect_vec(),
        by_tx_percentiles: dist_fee_by_index
            .iter()
            .map(|fee_point| fee_point.percentile as f32 / 100.0)
            .collect_vec(),
        by_cu: dist_fee_by_cu
            .iter()
            .map(|fee_point| fee_point.fees)
            .collect_vec(),
        by_cu_percentiles: dist_fee_by_cu
            .iter()
            .map(|fee_point| fee_point.percentile as f32 / 100.0)
            .collect_vec(),
    }
}

pub struct Percentiles {
    pub by_tx: Vec<u64>,
    pub by_tx_percentiles: Vec<f32>,
    pub by_cu: Vec<u64>,
    pub by_cu_percentiles: Vec<f32>,
}

#[allow(dead_code)]
impl Percentiles {
    fn get_fees_by_tx(&self, percentile: f32) -> Option<u64> {
        zip(&self.by_tx_percentiles, &self.by_tx)
            .find(|(&p, _cu)| p == percentile)
            .map(|(_p, &cu)| cu)
    }

    fn get_fees_cu(&self, percentile: f32) -> Option<u64> {
        zip(&self.by_cu_percentiles, &self.by_cu)
            .find(|(&p, _cu)| p == percentile)
            .map(|(_p, &cu)| cu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_supp_info() {
        let prio_fees_in_block = vec![
            PrioFeesData::from((2, 2)),
            PrioFeesData::from((4, 4)),
            PrioFeesData::from((5, 5)),
            PrioFeesData::from((3, 3)),
            PrioFeesData::from((1, 1)),
        ];
        let supp_info = calculate_supp_percentiles(&prio_fees_in_block).by_tx;
        assert_eq!(supp_info[0], 1);
        assert_eq!(supp_info[10], 3);
        assert_eq!(supp_info[15], 4);
        assert_eq!(supp_info[18], 5);
        assert_eq!(supp_info[20], 5);
    }

    #[test]
    fn test_calculate_supp_info_by_cu() {
        // total of 20000 CU where consumed
        let prio_fees_in_block = vec![
            PrioFeesData::from((100, 10000)),
            PrioFeesData::from((200, 10000)),
        ];
        let Percentiles {
            by_cu,
            by_cu_percentiles,
            ..
        } = calculate_supp_percentiles(&prio_fees_in_block);
        assert_eq!(by_cu_percentiles[10], 0.5);
        assert_eq!(by_cu[10], 100); // need more than 100 to beat 50% of the CU
        assert_eq!(by_cu[11], 200); // need more than 200 to beat 55% of the CU
        assert_eq!(by_cu[20], 200); // need more than 200 to beat 100% of the CU
    }

    #[test]
    fn test_empty_array() {
        let prio_fees_in_block = vec![];
        let supp_info = calculate_supp_percentiles(&prio_fees_in_block).by_tx;
        assert_eq!(supp_info[0], 0);
    }
    #[test]
    fn test_zeros() {
        let prio_fees_in_block = vec![PrioFeesData::from((0, 0)), PrioFeesData::from((0, 0))];
        let supp_info = calculate_supp_percentiles(&prio_fees_in_block).by_cu;
        assert_eq!(supp_info[0], 0);
    }

    #[test]
    fn test_statisticshowto() {
        let prio_fees_in_block = vec![
            PrioFeesData::from((30, 1)),
            PrioFeesData::from((33, 2)),
            PrioFeesData::from((43, 3)),
            PrioFeesData::from((53, 4)),
            PrioFeesData::from((56, 5)),
            PrioFeesData::from((67, 6)),
            PrioFeesData::from((68, 7)),
            PrioFeesData::from((72, 8)),
        ];
        let supp_info = calculate_supp_percentiles(&prio_fees_in_block);
        assert_eq!(supp_info.by_tx[5], 43);
        assert_eq!(supp_info.by_tx_percentiles[5], 0.25);
        assert_eq!(supp_info.by_cu_percentiles[20], 1.0);
        assert_eq!(supp_info.by_cu[20], 72);
    }

    #[test]
    fn test_simple_non_integer_index() {
        // Messwerte: 3 – 5 – 5 – 6 – 7 – 7 – 8 – 10 – 10
        // In diesem Fall lautet es also 5.
        let values = vec![
            PrioFeesData::from((3, 1)),
            PrioFeesData::from((5, 2)),
            PrioFeesData::from((5, 3)),
            PrioFeesData::from((6, 4)),
            PrioFeesData::from((7, 5)),
            PrioFeesData::from((7, 6)),
            PrioFeesData::from((8, 7)),
            PrioFeesData::from((10, 8)),
            PrioFeesData::from((10, 9)),
        ];

        let supp_info = calculate_supp_percentiles(&values);

        assert_eq!(supp_info.by_tx_percentiles[4], 0.20);
        assert_eq!(supp_info.by_tx[5], 5);
        assert_eq!(supp_info.by_cu_percentiles[19], 0.95);
        assert_eq!(supp_info.by_cu_percentiles[20], 1.0);
        assert_eq!(supp_info.by_cu[19], 10);
        assert_eq!(supp_info.by_cu[20], 10);
    }

    #[test]
    fn test_large_list() {
        let prio_fees_in_block = (0..1000).map(|x| PrioFeesData::from((x, x))).collect_vec();
        let supp_info = calculate_supp_percentiles(&prio_fees_in_block);
        assert_eq!(supp_info.by_tx[19], 950);
        assert_eq!(supp_info.by_tx_percentiles[19], 0.95);
    }
}
