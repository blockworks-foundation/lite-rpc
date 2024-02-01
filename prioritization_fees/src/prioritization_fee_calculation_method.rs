use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum PrioritizationFeeCalculationMethod {
    #[default]
    Latest,
    LastNBlocks(usize),
    Unknown,
}

impl<'de> Deserialize<'de> for PrioritizationFeeCalculationMethod {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s == "latest" {
            Ok(PrioritizationFeeCalculationMethod::Latest)
        } else if s.starts_with("last_n_blocks") {
            // should be of format last(n)
            let Ok(nb) = s
                .replace("last_n_blocks(", "")
                .replace(')', "")
                .parse::<usize>()
            else {
                return Ok(PrioritizationFeeCalculationMethod::Unknown);
            };
            Ok(PrioritizationFeeCalculationMethod::LastNBlocks(nb))
        } else {
            return Ok(PrioritizationFeeCalculationMethod::Unknown);
        }
    }
}
