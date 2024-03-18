pub fn histogram(values: &[f64], bins: usize) -> Vec<(f64, usize)> {
    assert!(bins >= 2);
    let mut bucket: Vec<usize> = vec![0; bins];

    let mut min = std::f64::MAX;
    let mut max = std::f64::MIN;
    for val in values {
        min = min.min(*val);
        max = max.max(*val);
    }
    let step = (max - min) / (bins - 1) as f64;

    for &v in values {
        let i = std::cmp::min(((v - min) / step).ceil() as usize, bins - 1);
        bucket[i] += 1;
    }

    bucket
        .into_iter()
        .enumerate()
        .map(|(i, v)| (min + step * i as f64, v))
        .collect()
}
