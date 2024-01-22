use crate::rpc_data::{FeePoint, PrioFeesStats};
use itertools::Itertools;
use std::collections::HashMap;

/// `quantile` function is the same as the median if q=50, the same as the minimum if q=0 and the same as the maximum if q=100.

pub fn calculate_supp_stats(
    // Vec(prioritization_fees, cu_consumed)
    prio_fees_in_block: &Vec<(u64, u64)>,
) -> PrioFeesStats {
    let prio_fees_in_block = if prio_fees_in_block.is_empty() {
        vec![(0, 0)]
    } else {
        // sort by prioritization fees
        prio_fees_in_block.iter().sorted_by_key(|(prio, _cu)| prio).cloned().collect_vec()
    };

    // get stats by transaction
    let dist_fee_by_index: Vec<FeePoint> = (0..=100)
        .step_by(5)
        .map(|p| {
            let prio_fee = {
                let index = prio_fees_in_block.len() * p / 100;
                let cap_index = index.min(prio_fees_in_block.len() - 1);
                prio_fees_in_block[cap_index].0
            };
            FeePoint {
                p: p as u32,
                v: prio_fee,
            }
        })
        .collect_vec();

    // get stats by CU
    // e.g. 95 -> 3000
    let mut dist_fee_by_cu: HashMap<i32, u64> = HashMap::new();
    let cu_sum: u64 = prio_fees_in_block.iter().map(|x| x.1).sum();
    let mut agg: u64 = 0;
    for (prio, cu) in prio_fees_in_block {
        agg += cu;
        for p in (0..=100).step_by(5) {
            if !dist_fee_by_cu.contains_key(&p) && agg >= (cu_sum as f64 * p as f64 / 100.0) as u64
            {
                dist_fee_by_cu.insert(p, prio);
            }
        }
    }

    // e.g. (p0, 0), (p5, 100), (p10, 200), ..., (p95, 3000), (p100, 3000)
    let dist_fee_by_cu: Vec<FeePoint> = dist_fee_by_cu
        .into_iter()
        .sorted_by_key(|(p, _)| *p)
        .map(|(p, fees)| FeePoint {
            p: p as u32,
            v: fees,
        })
        .collect_vec();

    PrioFeesStats {
        by_tx: dist_fee_by_index
            .iter()
            .map(|fee_point| fee_point.v)
            .collect_vec(),
        by_tx_percentiles: dist_fee_by_index
            .iter()
            .map(|fee_point| fee_point.p as f32 / 100.0)
            .collect_vec(),
        by_cu: dist_fee_by_cu
            .iter()
            .map(|fee_point| fee_point.v)
            .collect_vec(),
        by_cu_percentiles: dist_fee_by_cu
            .iter()
            .map(|fee_point| fee_point.p as f32 / 100.0)
            .collect_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_supp_info() {
        let prio_fees_in_block = vec![(2, 2), (4, 4), (5, 5), (3, 3), (1, 1)];
        let supp_info = calculate_supp_stats(&prio_fees_in_block).by_tx;
        assert_eq!(supp_info[0], 1);
        assert_eq!(supp_info[10], 3);
        assert_eq!(supp_info[15], 4);
        assert_eq!(supp_info[18], 5);
        assert_eq!(supp_info[20], 5);
    }

    #[test]
    fn test_empty_array() {
        let prio_fees_in_block = vec![];
        let supp_info = calculate_supp_stats(&prio_fees_in_block).by_tx;
        assert_eq!(supp_info[0], 0);
    }

    #[test]
    fn test_statisticshowto() {
        let prio_fees_in_block = vec![
            (30, 1),
            (33, 2),
            (43, 3),
            (53, 4),
            (56, 5),
            (67, 6),
            (68, 7),
            (72, 8),
        ];
        let supp_info = calculate_supp_stats(&prio_fees_in_block);
        assert_eq!(supp_info.by_tx[5], 43);
        assert_eq!(supp_info.by_tx_percentiles[5], 0.25);
    }

    #[test]
    fn test_large_list() {
        let prio_fees_in_block: Vec<(u64, u64)> = (0..1000).map(|x| (x, x)).collect();
        let supp_info = calculate_supp_stats(&prio_fees_in_block);
        assert_eq!(supp_info.by_tx[19], 950);
        assert_eq!(supp_info.by_tx_percentiles[19], 0.95);
    }
}
