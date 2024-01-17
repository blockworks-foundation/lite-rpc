use crate::rpc_data::PrioFeesStats;
use itertools::Itertools;
use std::collections::HashMap;

pub fn calculate_supp_stats(
    // Vec(prioritization_fees, cu_consumed)
    prio_fees_in_block: &Vec<(u64, u64)>,
) -> PrioFeesStats {
    let mut prio_fees_in_block = if prio_fees_in_block.is_empty() {
        // TODO is that smart?
        vec![(0, 0)]
    } else {
        prio_fees_in_block.clone()
    };
    prio_fees_in_block.sort_by(|a, b| a.0.cmp(&b.0));

    let median_index = prio_fees_in_block.len() / 2;
    let p75_index = prio_fees_in_block.len() * 75 / 100;
    let p90_index = prio_fees_in_block.len() * 90 / 100;
    let p_min = prio_fees_in_block[0].0;
    let p_median = prio_fees_in_block[median_index].0;
    let p_75 = prio_fees_in_block[p75_index].0;
    let p_90 = prio_fees_in_block[p90_index].0;
    let p_max = prio_fees_in_block.last().map(|x| x.0).unwrap();

    let fine_percentiles: HashMap<String, u64> = (0..=100)
        .step_by(5)
        .map(|percent| percent)
        .map(|x| {
            let prio_fee = if x == 100 {
                prio_fees_in_block.last().unwrap().0
            } else {
                let index = prio_fees_in_block.len() * x / 100;
                prio_fees_in_block[index].0
            };
            (format!("p{}", x), prio_fee)
        })
        .into_group_map_by(|x| x.0.clone())
        .into_iter()
        .map(|(k, v)| (k, v.iter().exactly_one().unwrap().1))
        .collect();

    assert_eq!(p_min, *fine_percentiles.get("p0").unwrap());
    assert_eq!(p_median, *fine_percentiles.get("p50").unwrap());
    assert_eq!(p_75, *fine_percentiles.get("p75").unwrap());
    assert_eq!(p_90, *fine_percentiles.get("p90").unwrap());
    assert_eq!(p_max, *fine_percentiles.get("p100").unwrap());

    PrioFeesStats {
        p_min,
        p_median,
        p_75,
        p_90,
        p_max,
        fine_percentiles,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_supp_info() {
        let prio_fees_in_block = vec![(2, 2), (4, 4), (5, 5), (3, 3), (1, 1)];
        let supp_info = calculate_supp_stats(&prio_fees_in_block);
        assert_eq!(supp_info.p_min, 1);
        assert_eq!(supp_info.p_median, 3);
        assert_eq!(supp_info.p_75, 4);
        assert_eq!(supp_info.p_90, 5);
        assert_eq!(supp_info.p_max, 5);
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
        assert_eq!(supp_info.fine_percentiles.get("p25").unwrap(), &43);
    }

    #[test]
    fn test_large_list() {
        let prio_fees_in_block: Vec<(u64, u64)> = (0..1000).map(|x| (x, x)).collect();
        let supp_info = calculate_supp_stats(&prio_fees_in_block);
        assert_eq!(supp_info.fine_percentiles.get("p95").unwrap(), &950);
    }
}
