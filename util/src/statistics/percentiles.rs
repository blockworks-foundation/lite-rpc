use itertools::Itertools;
use std::fmt::Display;
use std::iter::zip;

// return two arrays of same size mapping each percentile to a value
pub struct Percentiles {
    // percentile in range 0.0..1.0
    pub p: Vec<f32>,
    // value
    pub v: Vec<f64>,
}

#[allow(dead_code)]
impl Percentiles {
    fn get_bucket_value(&self, percentile: f32) -> Option<f64> {
        zip(&self.p, &self.v)
            .find(|(&p, _v)| p == percentile)
            .map(|(_p, &v)| v)
    }
}

impl Display for Percentiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let array = zip(&self.p, &self.v)
            .map(|(p, v)| format!("({:.2},{})", p, v))
            .join(",");
        write!(f, "Percentiles: {}", array)
    }
}

pub fn calculate_percentiles(input: &[f64]) -> Percentiles {
    if input.is_empty() {
        // note: percentile for empty array is undefined
        return Percentiles {
            p: vec![],
            v: vec![],
        };
    }

    // sort by prioritization fees
    let is_monotonic = input.windows(2).all(|v| v[0] <= v[1]);
    assert!(is_monotonic, "array of values must be sorted");

    let i_percentiles = (0..=100).step_by(5).collect_vec();

    let mut percentiles = Vec::with_capacity(i_percentiles.len());
    let mut bucket_values = Vec::with_capacity(i_percentiles.len());
    for p in i_percentiles {
        let value = {
            let index = input.len() * p / 100;
            let cap_index = index.min(input.len() - 1);
            input[cap_index]
        };

        percentiles.push(p as f32 / 100.0);
        bucket_values.push(value);
    }

    Percentiles {
        p: percentiles,
        v: bucket_values,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_percentiles() {
        let mut values = vec![2.0, 4.0, 5.0, 3.0, 1.0];
        values.sort_by_key(|&x| (x * 100.0) as i64);
        let result = calculate_percentiles(&values);
        let percentiles = &result.v;
        assert_eq!(percentiles[0], 1.0);
        assert_eq!(percentiles[10], 3.0);
        assert_eq!(percentiles[15], 4.0);
        assert_eq!(percentiles[18], 5.0);
        assert_eq!(percentiles[20], 5.0);
        assert_eq!(
            "Percentiles: \
            (0.00,1),(0.05,1),(0.10,1),(0.15,1),(0.20,2),\
            (0.25,2),(0.30,2),(0.35,2),(0.40,3),(0.45,3),\
            (0.50,3),(0.55,3),(0.60,4),(0.65,4),(0.70,4),\
            (0.75,4),(0.80,5),(0.85,5),(0.90,5),(0.95,5),\
            (1.00,5)",
            format!("{}", &result)
        );
    }

    #[test]
    fn test_empty_array() {
        let values = vec![];
        let percentiles = calculate_percentiles(&values).v;
        // note: this is controversal
        assert!(percentiles.is_empty());
    }

    #[test]
    fn test_statisticshowto() {
        let values = vec![30.0, 33.0, 43.0, 53.0, 56.0, 67.0, 68.0, 72.0];
        let percentiles = calculate_percentiles(&values);
        assert_eq!(percentiles.v[5], 43.0);
        assert_eq!(percentiles.p[5], 0.25);
        assert_eq!(percentiles.get_bucket_value(0.25), Some(43.0));
    }

    #[test]
    fn test_simple_non_integer_index() {
        // Messwerte: 3 – 5 – 5 – 6 – 7 – 7 – 8 – 10 – 10
        // In diesem Fall lautet es also 5.
        let values = vec![3.0, 5.0, 5.0, 6.0, 7.0, 7.0, 8.0, 10.0, 10.0];

        let percentiles = calculate_percentiles(&values);
        assert_eq!(percentiles.p[4], 0.20);
        assert_eq!(percentiles.v[5], 5.0);
    }

    #[test]
    fn test_large_list() {
        let values = (0..1000).map(|i| i as f64).collect_vec();
        let percentiles = calculate_percentiles(&values);
        assert_eq!(percentiles.v[19], 950.0);
        assert_eq!(percentiles.p[19], 0.95);
    }
}
