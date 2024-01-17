use crate::rpc_data::PrioFeesStats;
use itertools::Itertools;
use std::collections::HashMap;

pub fn calculate_supp_stats(
    // Vec(prioritization_fees, cu_consumed)
    prio_fees_in_block: &Vec<(u64, u64)>,
) -> PrioFeesStats {
    let mut prio_fees_in_block = if prio_fees_in_block.is_empty() {
        vec![(0, 0)]
    } else {
        prio_fees_in_block.clone()
    };
    // sort by prioritization fees
    prio_fees_in_block.sort_by(|a, b| a.0.cmp(&b.0));

    // get stats by transaction
    let dist_fee_by_index: Vec<(String, u64)> =
        (0..=100).step_by(5)
        .map(|p| {
            let prio_fee = if p == 100 {
                prio_fees_in_block.last().unwrap().0
            } else {
                let index = prio_fees_in_block.len() * p / 100;
                prio_fees_in_block[index].0
            };
            (format!("p_{}", p), prio_fee)
        })
        .collect_vec();


    // get stats by CU
    // e.g. 95 -> 3000
    let mut dist_fee_by_cu: HashMap<i32, u64> = HashMap::new();
    let cu_sum: u64 = prio_fees_in_block.iter().map(|x| x.1).sum();
    let mut agg: u64 = 0;
    for (prio, cu) in prio_fees_in_block {
        agg = agg + cu;
        for p in (0..=100).step_by(5) {
            if !dist_fee_by_cu.contains_key(&p) {
                if agg > (cu_sum as f64 * p as f64 / 100.0) as u64 {
                    dist_fee_by_cu.insert(p, prio);
                }
            }
        }
    }

    // e.g. (p0, 0), (p5, 100), (p10, 200), ..., (p95, 3000), (p100, 3000)
    let dist_fee_by_cu: Vec<(String, u64)> =
        dist_fee_by_cu
        .into_iter()
        .sorted_by_key(|(p, _)| *p)
        .map(|(p, fees)| (format!("p_{}", p), fees))
        .collect_vec();

    PrioFeesStats {
        dist_fee_by_index,
        dist_fee_by_cu,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_supp_info() {
        let prio_fees_in_block = vec![(2, 2), (4, 4), (5, 5), (3, 3), (1, 1)];
        let supp_info = calculate_supp_stats(&prio_fees_in_block).dist_fee_by_index;
        assert_eq!(supp_info[0], ("p_0".to_string(), 1));
        assert_eq!(supp_info[10], ("p_50".to_string(), 3));
        assert_eq!(supp_info[15], ("p_75".to_string(), 4));
        assert_eq!(supp_info[18], ("p_90".to_string(), 5));
        assert_eq!(supp_info[20], ("p_100".to_string(), 5));
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
        assert_eq!(supp_info.dist_fee_by_index[5], ("p_25".to_string(), 43));
    }

    #[test]
    fn test_large_list() {
        let prio_fees_in_block: Vec<(u64, u64)> = (0..1000).map(|x| (x, x)).collect();
        let supp_info = calculate_supp_stats(&prio_fees_in_block);
        assert_eq!(supp_info.dist_fee_by_index[19], ("p_95".to_string(), 950));
    }
}
