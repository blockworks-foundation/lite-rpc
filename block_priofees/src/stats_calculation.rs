use crate::rpc_data::{FeePoint, PrioFeesStats};
use itertools::Itertools;
use std::collections::HashMap;
use std::iter::zip;

/// `quantile` function is the same as the median if q=50, the same as the minimum if q=0 and the same as the maximum if q=100.

pub fn calculate_supp_percentiles(
    // Vec(prioritization_fees, cu_consumed)
    prio_fees_in_block: &Vec<(u64, u64)>,
) -> Percentiles {
    let prio_fees_in_block = if prio_fees_in_block.is_empty() {
        // note: percentile for empty array is undefined
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

    let dist_fee_by_cu: Vec<FeePoint> = dist_fee_by_cu
        .into_iter()
        .sorted_by_key(|(p, _)| *p)
        .map(|(p, fees)| FeePoint {
            p: p as u32,
            v: fees,
        })
        .collect_vec();

    Percentiles {
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


pub fn calculate_supp_percentiles2(
    // Vec(prioritization_fees, cu_consumed)
    prio_fees_in_block: &Vec<(u64, u64)>,
) -> Percentiles {
    let prio_fees_in_block = if prio_fees_in_block.is_empty() {
        // note: percentile for empty array is undefined
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
    let mut p = 0;
    for (prio, cu) in &prio_fees_in_block {
        agg += cu;
        // write p's as long as agg beats the current highest
        while agg >= (cu_sum as f64 * p as f64 / 100.0) as u64 && p <= 100 {
            dist_fee_by_cu.insert(p, *prio);
            p += 5;
        }
    }
    let dist_fee_by_cu: Vec<FeePoint> = dist_fee_by_cu
        .into_iter()
        .sorted_by_key(|(p, _)| *p)
        .map(|(p, fees)| FeePoint {
            p: p as u32,
            v: fees,
        })
        .collect_vec();

    Percentiles {
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


pub struct Percentiles {
    pub by_tx: Vec<u64>,
    pub by_tx_percentiles: Vec<f32>,
    pub by_cu: Vec<u64>,
    pub by_cu_percentiles: Vec<f32>,
}

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
    use std::iter::zip;
    use proptest::collection::{hash_set, vec};
    use proptest::num::u64::BinarySearch;
    use proptest::prelude::{ProptestConfig, Strategy};
    use proptest::{num, proptest};
    use super::*;

    #[test]
    fn test_calculate_supp_info() {
        let prio_fees_in_block = vec![(2, 2), (4, 4), (5, 5), (3, 3), (1, 1)];
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
        let prio_fees_in_block = vec![(100, 10000), (200, 10000)];
        let Percentiles { by_cu, by_cu_percentiles, .. } = calculate_supp_percentiles(&prio_fees_in_block);
        assert_eq!(by_cu_percentiles[10], 0.5);
        assert_eq!(by_cu[10], 100); // need more than 100 to beat 50% of the CU
        assert_eq!(by_cu[11], 200); // need more than 200 to beat 55% of the CU
        assert_eq!(by_cu[20], 200); // need more than 200 to beat 100% of the CU

    }


    #[test]
    fn test_empty_newimpl() {
        let prio_fees_in_block = vec![];
        let supp_info = calculate_supp_percentiles2(&prio_fees_in_block).by_cu;
        assert_eq!(supp_info[0], 0);
    }

    #[test]
    fn test_zeros_newimpl() {
        let prio_fees_in_block = vec![(0, 0), (0, 0)];
        let supp_info = calculate_supp_percentiles2(&prio_fees_in_block).by_cu;
        assert_eq!(supp_info[0], 0);
    }

    #[test]
    fn compareoldnew() {
        // total of 20000 CU where consumed
        let prio_fees_in_block = vec![(100, 10000), (200, 10000)];
        {
            let Percentiles { by_cu, by_cu_percentiles, .. } = calculate_supp_percentiles(&prio_fees_in_block);
            zip(&by_cu, &by_cu_percentiles).for_each(|(cu, p)| println!("{} {}", cu, p));

            let percentiles = calculate_supp_percentiles(&prio_fees_in_block);
            assert_eq!(percentiles.get_fees_cu(0.5), Some(100));
            assert_eq!(percentiles.get_fees_cu(0.55), Some(200));


        }
        println!("---");
        {
            let Percentiles { by_cu, by_cu_percentiles, .. } = calculate_supp_percentiles2(&prio_fees_in_block);
            zip(&by_cu, &by_cu_percentiles).for_each(|(cu, p)| println!("{} {}", cu, p));
        }

        let old = calculate_supp_percentiles(&prio_fees_in_block);
        let new = calculate_supp_percentiles2(&prio_fees_in_block);
        assert_eq!(old.by_cu_percentiles, new.by_cu_percentiles);
        assert_eq!(old.by_cu, new.by_cu);
    }



    #[test]
    fn test_empty_array() {
        let prio_fees_in_block = vec![];
        let supp_info = calculate_supp_percentiles2(&prio_fees_in_block).by_tx;
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
        let supp_info = calculate_supp_percentiles(&prio_fees_in_block);
        assert_eq!(supp_info.by_tx[5], 43);
        assert_eq!(supp_info.by_tx_percentiles[5], 0.25);
    }

    #[test]
    fn test_simple_non_integer_index() {
        // Messwerte: 3 – 5 – 5 – 6 – 7 – 7 – 8 – 10 – 10
        // In diesem Fall lautet es also 5.
        let values = vec![
            (3, 1),
            (5, 2),
            (5, 3),
            (6, 4),
            (7, 5),
            (7, 6),
            (8, 7),
            (10, 8),
            (10, 9),
        ];

        let supp_info = calculate_supp_percentiles(&values);


        assert_eq!(supp_info.by_tx_percentiles[4], 0.20);
        assert_eq!(supp_info.by_tx[5], 5);


    }

    #[test]
    fn test_large_list() {
        let prio_fees_in_block: Vec<(u64, u64)> = (0..1000).map(|x| (x, x)).collect();
        let supp_info = calculate_supp_percentiles(&prio_fees_in_block);
        assert_eq!(supp_info.by_tx[19], 950);
        assert_eq!(supp_info.by_tx_percentiles[19], 0.95);
    }

    fn vec_of_prio_and_cu() -> impl Strategy<Value = Vec<(u64,u64)>> {
        const N: u16 = 10;

        let length = 0..N as usize;
        length.prop_flat_map(vec_from_length)
    }

    fn vec_from_length(length: usize) -> impl Strategy<Value = Vec<(u64,u64)>> {
        const N: u16 = 10;
        vec((0..1000u64, 0..1000u64), length)
    }
    proptest! {
        #[test]
        fn prop(prio_fees_in_block in vec_of_prio_and_cu()) {
            let Percentiles { by_cu: by_cu1, .. } = calculate_supp_percentiles(&prio_fees_in_block);
            let Percentiles { by_cu: by_cu2, .. } = calculate_supp_percentiles2(&prio_fees_in_block);
            assert_eq!(by_cu1, by_cu2);
        }
  }

}

fn reverse<T: Clone>(xs: &[T]) -> Vec<T> {
    let mut rev = vec!();
    for x in xs.iter() {
        rev.insert(0, x.clone())
    }
    rev
}


