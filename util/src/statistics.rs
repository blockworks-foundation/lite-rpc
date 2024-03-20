
pub fn mean(data: &[f32]) -> Option<f32> {
    let sum = data.iter().sum::<f32>() as f32;
    let count = data.len();

    match count {
        positive if positive > 0 => Some(sum / count as f32),
        _ => None,
    }
}

pub fn std_deviation(data: &[f32]) -> Option<f32> {
    match (mean(data), data.len()) {
        (Some(data_mean), count) if count > 0 => {
            let variance = data.iter().map(|value| {
                let diff = data_mean - (*value as f32);

                diff * diff
            }).sum::<f32>() / count as f32;

            Some(variance.sqrt())
        },
        _ => None
    }
}

#[test]
fn test_mean() {
    let data = [1.0, 2.0, 3.0, 4.0, 5.0];
    assert_eq!(mean(&data), Some(3.0));
}

#[test]
fn test_std_deviation() {
    let data = [1.0, 2.0, 3.0, 4.0, 5.0];
    assert_eq!(std_deviation(&data), Some(1.4142135));
}

