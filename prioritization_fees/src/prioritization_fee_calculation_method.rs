use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum PrioritizationFeeCalculationMethod {
    #[default]
    Latest,
    Last(usize),
}
