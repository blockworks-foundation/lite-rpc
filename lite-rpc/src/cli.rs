use std::env;

use crate::{
    DEFAULT_FANOUT_SIZE, DEFAULT_GRPC_ADDR, DEFAULT_RETRY_TIMEOUT, DEFAULT_RPC_ADDR,
    DEFAULT_WS_ADDR, MAX_RETRIES,
};
use anyhow::Context;
use clap::Parser;
use dotenv::dotenv;
use solana_lite_rpc_history::postgres::postgres_config::PostgresSessionConfig;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// config.json
    #[arg(short, long)]
    pub config: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Config {
    #[serde(default = "Config::default_rpc_addr")]
    pub rpc_addr: String,
    #[serde(default = "Config::default_ws_addr")]
    pub ws_addr: String,
    #[serde(default = "Config::default_lite_rpc_http_addr")]
    pub lite_rpc_http_addr: String,
    #[serde(default = "Config::default_lite_rpc_ws_addr")]
    pub lite_rpc_ws_addr: String,
    #[serde(default = "Config::default_fanout_size")]
    pub fanout_size: u64,
    // Identity keypair path
    #[serde(default)]
    pub identity_keypair: Option<String>,
    #[serde(default = "Config::default_prometheus_addr")]
    pub prometheus_addr: String,
    #[serde(default = "Config::default_maximum_retries_per_tx")]
    pub maximum_retries_per_tx: usize,
    #[serde(default = "Config::default_transaction_retry_after_secs")]
    pub transaction_retry_after_secs: u64,
    #[serde(default)]
    pub quic_proxy_addr: Option<String>,
    #[serde(default)]
    pub use_grpc: bool,
    #[serde(default = "Config::default_grpc_addr")]
    pub grpc_addr: String,
    #[serde(default)]
    pub grpc_x_token: Option<String>,
    /// postgres config
    #[serde(default)]
    pub postgres: Option<PostgresSessionConfig>,
}

impl Config {
    pub async fn load() -> anyhow::Result<Self> {
        dotenv().ok();

        let args = Args::parse();

        let config_path = if args.config.is_some() {
            args.config
        } else {
            let default_config_path = "config.json";

            // check if config.json exists in current directory
            if tokio::fs::metadata(default_config_path).await.is_err() {
                None
            } else {
                Some(default_config_path.to_string())
            }
        };

        let config = if let Some(config_path) = config_path {
            tokio::fs::read_to_string(config_path)
                .await
                .context("Error reading config file")?
        } else {
            "{}".to_string()
        };

        let mut config: Config =
            serde_json::from_str(&config).context("Error parsing config file")?;

        config.rpc_addr = env::var("RPC_ADDR").unwrap_or(config.rpc_addr);

        config.ws_addr = env::var("WS_ADDR").unwrap_or(config.ws_addr);

        config.lite_rpc_http_addr =
            env::var("LITE_RPC_HTTP_ADDR").unwrap_or(config.lite_rpc_http_addr);

        config.lite_rpc_ws_addr = env::var("LITE_RPC_WS_ADDR").unwrap_or(config.lite_rpc_ws_addr);

        config.fanout_size = env::var("FANOUT_SIZE")
            .map(|size| size.parse().unwrap())
            .unwrap_or(config.fanout_size);

        // IDENTITY env sets value of identity_keypair

        // config.identity_keypair = env::var("IDENTITY")
        //     .map(Some)
        //     .unwrap_or(config.identity_keypair);

        config.prometheus_addr = env::var("PROMETHEUS_ADDR").unwrap_or(config.prometheus_addr);

        config.maximum_retries_per_tx = env::var("MAX_RETRIES")
            .map(|max| max.parse().unwrap())
            .unwrap_or(config.maximum_retries_per_tx);

        config.transaction_retry_after_secs = env::var("RETRY_TIMEOUT")
            .map(|secs| secs.parse().unwrap())
            .unwrap_or(config.transaction_retry_after_secs);

        config.quic_proxy_addr = env::var("QUIC_PROXY_ADDR").ok();

        config.use_grpc = env::var("USE_GRPC")
            .map(|_| true)
            .unwrap_or(config.use_grpc);

        config.grpc_addr = env::var("GRPC_ADDR").unwrap_or(config.grpc_addr);

        config.grpc_x_token = env::var("GRPC_X_TOKEN")
            .map(Some)
            .unwrap_or(config.grpc_x_token);

        config.postgres = PostgresSessionConfig::new_from_env()?.or(config.postgres);

        Ok(config)
    }

    pub fn lite_rpc_ws_addr() -> String {
        "[::]:8891".to_string()
    }

    pub fn default_lite_rpc_http_addr() -> String {
        "[::]:8890".to_string()
    }

    pub fn default_rpc_addr() -> String {
        DEFAULT_RPC_ADDR.to_string()
    }

    pub fn default_ws_addr() -> String {
        DEFAULT_WS_ADDR.to_string()
    }

    pub fn default_lite_rpc_ws_addr() -> String {
        "[::]:8891".to_string()
    }

    pub const fn default_fanout_size() -> u64 {
        DEFAULT_FANOUT_SIZE
    }

    pub fn default_prometheus_addr() -> String {
        "[::]:9091".to_string()
    }

    pub const fn default_maximum_retries_per_tx() -> usize {
        MAX_RETRIES
    }

    pub const fn default_transaction_retry_after_secs() -> u64 {
        DEFAULT_RETRY_TIMEOUT
    }

    pub fn default_grpc_addr() -> String {
        DEFAULT_GRPC_ADDR.to_string()
    }
}
