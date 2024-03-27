use itertools::Itertools;
use std::fmt::Display;
use std::iter::zip;

// #[derive(Clone, Copy, Debug, Default)]
pub struct Point {
    pub priority: f64,
    pub value: f64,
}

impl From<(f64, f64)> for Point {
    fn from((priority, cu_consumed): (f64, f64)) -> Self {
        Point {
            priority,
            value: cu_consumed,
        }
    }
}

// #[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct HistValue {
    pub percentile: f32,
    pub value: f64,
}

/// `quantile` function is the same as the median if q=50, the same as the minimum if q=0 and the same as the maximum if q=100.

pub fn calculate_percentiles(input: &[f64]) -> Percentiles {
    if input.is_empty() {
        // note: percentile for empty array is undefined
        return Percentiles {
            v: vec![],
            p: vec![],
        };
    }

    let is_monotonic = input.windows(2).all(|w| w[0] <= w[1]);
    assert!(is_monotonic, "array of values must be sorted");

    let p_step = 5;
    let i_percentiles = (0..=100).step_by(p_step).collect_vec();

    let mut bucket_values = Vec::with_capacity(i_percentiles.len());
    let mut percentiles = Vec::with_capacity(i_percentiles.len());
    for p in i_percentiles {
        let value = {
            let index = input.len() * p / 100;
            let cap_index = index.min(input.len() - 1);
            input[cap_index]
        };

        bucket_values.push(value);
        percentiles.push(p as f32 / 100.0);
    }

    Percentiles {
        v: bucket_values,
        p: percentiles,
    }
}

pub fn calculate_cummulative(values: &[Point]) -> PercentilesCummulative {
    if values.is_empty() {
        // note: percentile for empty array is undefined
        return PercentilesCummulative {
            bucket_values: vec![],
            percentiles: vec![],
        };
    }

    let is_monotonic = values.windows(2).all(|w| w[0].priority <= w[1].priority);
    assert!(is_monotonic, "array of values must be sorted");

    let value_sum: f64 = values.iter().map(|x| x.value).sum();
    let mut agg: f64 = values[0].value;
    let mut index = 0;
    let p_step = 5;

    let percentiles = (0..=100).step_by(p_step).map(|p| p as f64).collect_vec();

    let dist = percentiles
        .iter()
        .map(|percentile| {
            while agg < (value_sum * *percentile) / 100.0 {
                index += 1;
                agg += values[index].value;
            }
            let priority = values[index].priority;
            HistValue {
                percentile: *percentile as f32,
                value: priority,
            }
        })
        .collect_vec();

    PercentilesCummulative {
        bucket_values: dist.iter().map(|hv| hv.value).collect_vec(),
        percentiles: dist.iter().map(|hv| hv.percentile / 100.0).collect_vec(),
    }
}

pub struct Percentiles {
    // value
    pub v: Vec<f64>,
    // percentile in range 0.0..1.0
    pub p: Vec<f32>,
}

impl Display for Percentiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for i in 0..self.v.len() {
            write!(f, "p{}=>{} ", self.p[i] * 100.0, self.v[i])?;
        }
        Ok(())
    }
}

#[allow(dead_code)]
impl Percentiles {
    fn get_bucket_value(&self, percentile: f32) -> Option<f64> {
        zip(&self.p, &self.v)
            .find(|(&p, _v)| p == percentile)
            .map(|(_p, &v)| v)
    }
}

pub struct PercentilesCummulative {
    pub bucket_values: Vec<f64>,
    pub percentiles: Vec<f32>,
}

#[allow(dead_code)]
impl PercentilesCummulative {
    fn get_bucket_value(&self, percentile: f32) -> Option<f64> {
        zip(&self.percentiles, &self.bucket_values)
            .find(|(&p, _cu)| p == percentile)
            .map(|(_p, &cu)| cu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_percentiles() {
        let mut values = vec![2.0, 4.0, 5.0, 3.0, 1.0];
        values.sort_by_key(|&x| (x * 100.0) as i64);
        let percentiles = calculate_percentiles(&values).v;
        assert_eq!(percentiles[0], 1.0);
        assert_eq!(percentiles[10], 3.0);
        assert_eq!(percentiles[15], 4.0);
        assert_eq!(percentiles[18], 5.0);
        assert_eq!(percentiles[20], 5.0);
    }

    #[test]
    fn test_calculate_percentiles_by_cu() {
        // total of 20000 CU where consumed
        let values = vec![Point::from((100.0, 10000.0)), Point::from((200.0, 10000.0))];
        let PercentilesCummulative {
            bucket_values: by_cu,
            percentiles: by_cu_percentiles,
            ..
        } = calculate_cummulative(&values);
        assert_eq!(by_cu_percentiles[10], 0.5);
        assert_eq!(by_cu[10], 100.0); // need more than 100 to beat 50% of the CU
        assert_eq!(by_cu[11], 200.0); // need more than 200 to beat 55% of the CU
        assert_eq!(by_cu[20], 200.0); // need more than 200 to beat 100% of the CU
    }

    #[test]
    fn test_empty_array() {
        let values = vec![];
        let percentiles = calculate_percentiles(&values).v;
        // note: this is controversal
        assert!(percentiles.is_empty());
    }
    #[test]
    fn test_zeros() {
        let values = vec![Point::from((0.0, 0.0)), Point::from((0.0, 0.0))];
        let percentiles = calculate_cummulative(&values).bucket_values;
        assert_eq!(percentiles[0], 0.0);
    }

    #[test]
    fn test_statisticshowto() {
        let values = vec![30.0, 33.0, 43.0, 53.0, 56.0, 67.0, 68.0, 72.0];
        let percentiles = calculate_percentiles(&values);
        assert_eq!(percentiles.v[5], 43.0);
        assert_eq!(percentiles.p[5], 0.25);
        assert_eq!(percentiles.get_bucket_value(0.25), Some(43.0));

        let values = vec![
            Point::from((30.0, 1.0)),
            Point::from((33.0, 2.0)),
            Point::from((43.0, 3.0)),
            Point::from((53.0, 4.0)),
            Point::from((56.0, 5.0)),
            Point::from((67.0, 6.0)),
            Point::from((68.0, 7.0)),
            Point::from((72.0, 8.0)),
        ];
        let percentiles = calculate_cummulative(&values);
        assert_eq!(percentiles.percentiles[20], 1.0);
        assert_eq!(percentiles.bucket_values[20], 72.0);
    }

    #[test]
    fn test_simple_non_integer_index() {
        // Messwerte: 3 – 5 – 5 – 6 – 7 – 7 – 8 – 10 – 10
        // In diesem Fall lautet es also 5.
        let values = vec![3.0, 5.0, 5.0, 6.0, 7.0, 7.0, 8.0, 10.0, 10.0];

        let percentiles = calculate_percentiles(&values);
        assert_eq!(percentiles.p[4], 0.20);
        assert_eq!(percentiles.v[5], 5.0);

        let values = vec![
            Point::from((3.0, 1.0)),
            Point::from((5.0, 2.0)),
            Point::from((5.0, 3.0)),
            Point::from((6.0, 4.0)),
            Point::from((7.0, 5.0)),
            Point::from((7.0, 6.0)),
            Point::from((8.0, 7.0)),
            Point::from((10.0, 8.0)),
            Point::from((10.0, 9.0)),
        ];
        let percentiles = calculate_cummulative(&values);
        assert_eq!(percentiles.percentiles[19], 0.95);
        assert_eq!(percentiles.percentiles[20], 1.0);
        assert_eq!(percentiles.bucket_values[19], 10.0);
        assert_eq!(percentiles.bucket_values[20], 10.0);
    }

    #[test]
    fn test_large_list() {
        let values = (0..1000).map(|i| i as f64).collect_vec();
        let percentiles = calculate_percentiles(&values);
        assert_eq!(percentiles.v[19], 950.0);
        assert_eq!(percentiles.p[19], 0.95);
    }
}
