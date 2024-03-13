use crate::tx_size::TxSize;
use serde::Deserialize;
use std::fs::File;
use std::io::Read;

#[derive(Clone, Debug, Deserialize)]
pub struct BenchConfig {
    pub payer_path: String,
    pub lite_rpc_url: String,
    pub rpc_url: String,
    pub api_load: ApiLoadConfig,
    pub confirmation_rate: ConfirmationRateConfig,
    pub confirmation_slot: ConfirmationSlotConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ApiLoadConfig {
    pub time_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ConfirmationRateConfig {
    pub num_txns: usize,
    pub num_runs: usize,
    pub tx_size: TxSize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ConfirmationSlotConfig {
    pub tx_size: TxSize,
}

impl BenchConfig {
    pub fn load() -> anyhow::Result<Self> {
        let args: Vec<String> = std::env::args().collect();
        let mut file = File::open(&args[1])?;
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        Ok(toml::from_str(&contents)?)
    }
}
