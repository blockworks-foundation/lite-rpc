use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};
use std::net::SocketAddr;
use std::str::FromStr;
use std::{env, time::Duration};

use crate::postgres_logger::{self, PostgresSessionConfig};
use crate::{
    DEFAULT_FANOUT_SIZE, DEFAULT_GRPC_ADDR, DEFAULT_RETRY_TIMEOUT, DEFAULT_RPC_ADDR,
    DEFAULT_WS_ADDR, MAX_RETRIES,
};
use anyhow::Context;
use clap::Parser;
use dotenv::dotenv;
use solana_lite_rpc_services::quic_connection_utils::QuicConnectionParameters;
use solana_rpc_client_api::client_error::reqwest::Url;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// config.json
    #[arg(short = 'c', long)]
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
    #[serde(default)]
    pub calculate_leader_schedule_from_geyser: bool,
    #[serde(default = "Config::default_grpc_addr")]
    pub grpc_addr: String,
    #[serde(default)]
    pub grpc_x_token: Option<String>,

    #[serde(default)]
    pub grpc_addr2: Option<String>,
    #[serde(default)]
    pub grpc_x_token2: Option<String>,

    #[serde(default)]
    pub grpc_addr3: Option<String>,
    #[serde(default)]
    pub grpc_x_token3: Option<String>,

    #[serde(default)]
    pub grpc_addr4: Option<String>,
    #[serde(default)]
    pub grpc_x_token4: Option<String>,

    #[serde(default)]
    pub enable_grpc_stream_inspection: bool,

    /// postgres config
    #[serde(default)]
    pub postgres: Option<postgres_logger::PostgresSessionConfig>,

    #[serde(default)]
    pub max_number_of_connection: Option<usize>,

    #[serde(default)]
    pub address_lookup_tables_binary: Option<String>,

    #[serde(default)]
    pub enable_address_lookup_tables: Option<bool>,

    #[serde(default)]
    pub account_filters: Option<String>,

    #[serde(default)]
    pub enable_accounts_on_demand_accounts_service: Option<bool>,

    #[serde(default)]
    pub quic_connection_parameters: Option<QuicConnectionParameters>,
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

        SocketAddr::from_str(&config.lite_rpc_http_addr).expect("invalid LITE_RPC_HTTP_ADDR");
        SocketAddr::from_str(&config.lite_rpc_ws_addr).expect("invalid LITE_RPC_WS_ADDR");

        config.fanout_size = env::var("FANOUT_SIZE")
            .map(|size| size.parse().unwrap())
            .unwrap_or(config.fanout_size);

        // note: identity config is handled in load_identity_keypair
        // the behavior is different from the other config values as it does either take a file path or the keypair as json array

        config.prometheus_addr = env::var("PROMETHEUS_ADDR").unwrap_or(config.prometheus_addr);

        config.maximum_retries_per_tx = env::var("MAX_RETRIES")
            .map(|max| max.parse().unwrap())
            .unwrap_or(config.maximum_retries_per_tx);

        config.transaction_retry_after_secs = env::var("RETRY_TIMEOUT")
            .map(|secs| secs.parse().unwrap())
            .unwrap_or(config.transaction_retry_after_secs);

        config.quic_proxy_addr = env::var("QUIC_PROXY_ADDR").ok();

        config.use_grpc = env::var("USE_GRPC")
            .map(|value| value.parse::<bool>().unwrap())
            .unwrap_or(config.use_grpc);

        // source 1
        config.grpc_addr = env::var("GRPC_ADDR").unwrap_or(config.grpc_addr);
        config.grpc_x_token = env::var("GRPC_X_TOKEN")
            .map(Some)
            .unwrap_or(config.grpc_x_token);

        assert!(
            env::var("GRPC_ADDR1").is_err(),
            "use GRPC_ADDR instead of GRPC_ADDR1"
        );
        assert!(
            env::var("GRPC_X_TOKEN1").is_err(),
            "use GRPC_X_TOKEN instead of GRPC_X_TOKEN1"
        );

        // source 2
        config.grpc_addr2 = env::var("GRPC_ADDR2")
            .map(Some)
            .unwrap_or(config.grpc_addr2);
        config.grpc_x_token2 = env::var("GRPC_X_TOKEN2")
            .map(Some)
            .unwrap_or(config.grpc_x_token2);

        // source 3
        config.grpc_addr3 = env::var("GRPC_ADDR3")
            .map(Some)
            .unwrap_or(config.grpc_addr3);
        config.grpc_x_token3 = env::var("GRPC_X_TOKEN3")
            .map(Some)
            .unwrap_or(config.grpc_x_token3);

        // source 4
        config.grpc_addr4 = env::var("GRPC_ADDR4")
            .map(Some)
            .unwrap_or(config.grpc_addr4);
        config.grpc_x_token4 = env::var("GRPC_X_TOKEN4")
            .map(Some)
            .unwrap_or(config.grpc_x_token4);

        config.enable_grpc_stream_inspection = env::var("ENABLE_GRPC_STREAM_INSPECTION")
            .map(|value| value.parse::<bool>().expect("bool value"))
            .unwrap_or(config.enable_grpc_stream_inspection);

        config.max_number_of_connection = env::var("MAX_NB_OF_CONNECTIONS_WITH_LEADERS")
            .map(|x| x.parse().ok())
            .unwrap_or(config.max_number_of_connection);

        config.enable_address_lookup_tables = env::var("ENABLE_ADDRESS_LOOKUP_TABLES")
            .map(|value| value.parse::<bool>().unwrap())
            .ok()
            .or(config.enable_address_lookup_tables);

        config.address_lookup_tables_binary = env::var("ADDRESS_LOOKUP_TABLES_BINARY")
            .ok()
            .or(config.address_lookup_tables_binary);

        config.account_filters = env::var("ACCOUNT_FILTERS").ok().or(config.account_filters);

        config.enable_accounts_on_demand_accounts_service = env::var("ENABLE_ACCOUNT_ON_DEMAND")
            .map(|value| value.parse::<bool>().unwrap())
            .ok()
            .or(config.enable_accounts_on_demand_accounts_service);

        config.postgres = PostgresSessionConfig::new_from_env()?.or(config.postgres);
        config.quic_connection_parameters = config
            .quic_connection_parameters
            .or(quic_params_from_environment());
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

    pub fn get_grpc_sources(&self) -> Vec<GrpcSource> {
        let mut sources: Vec<GrpcSource> = vec![];

        sources.push(GrpcSource {
            addr: self.grpc_addr.clone(),
            x_token: self.grpc_x_token.clone(),
        });

        if self.grpc_addr2.is_some() {
            sources.push(GrpcSource {
                addr: self.grpc_addr2.clone().unwrap(),
                x_token: self.grpc_x_token2.clone(),
            });
        }

        if self.grpc_addr3.is_some() {
            sources.push(GrpcSource {
                addr: self.grpc_addr3.clone().unwrap(),
                x_token: self.grpc_x_token3.clone(),
            });
        }

        if self.grpc_addr4.is_some() {
            sources.push(GrpcSource {
                addr: self.grpc_addr4.clone().unwrap(),
                x_token: self.grpc_x_token4.clone(),
            });
        }

        sources
    }
}

#[derive(Clone)]
pub struct GrpcSource {
    pub addr: String,
    pub x_token: Option<String>,
}

impl Display for GrpcSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GrpcSource {} (x-token {})",
            url_obfuscate_api_token(&self.addr),
            obfuscate_token(&self.x_token)
        )
    }
}

impl Debug for GrpcSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

/// obfuscate urls with api token like http://mango.rpcpool.com/a991fba00fagbad
fn url_obfuscate_api_token(url: &str) -> Cow<str> {
    if let Ok(mut parsed) = Url::parse(url) {
        if parsed.path() == "/" {
            return Cow::Borrowed(url);
        } else {
            parsed.set_path("omitted-secret");
            Cow::Owned(parsed.to_string())
        }
    } else {
        Cow::Borrowed(url)
    }
}

fn obfuscate_token(token: &Option<String>) -> String {
    match token {
        None => "n/a".to_string(),
        Some(token) => {
            let mut token = token.clone();
            token.truncate(5);
            token += "...";
            token
        }
    }
}

fn quic_params_from_environment() -> Option<QuicConnectionParameters> {
    let mut quic_connection_parameters = QuicConnectionParameters::default();

    quic_connection_parameters.connection_timeout = env::var("QUIC_CONNECTION_TIMEOUT_MILLIS")
        .map(|millis| Duration::from_millis(millis.parse().unwrap()))
        .unwrap_or(quic_connection_parameters.connection_timeout);

    quic_connection_parameters.unistream_timeout = env::var("QUIC_UNISTREAM_TIMEOUT_MILLIS")
        .map(|millis| Duration::from_millis(millis.parse().unwrap()))
        .unwrap_or(quic_connection_parameters.unistream_timeout);

    quic_connection_parameters.write_timeout = env::var("QUIC_WRITE_TIMEOUT_MILLIS")
        .map(|millis| Duration::from_millis(millis.parse().unwrap()))
        .unwrap_or(quic_connection_parameters.write_timeout);

    quic_connection_parameters.finalize_timeout = env::var("QUIC_FINALIZE_TIMEOUT_MILLIS")
        .map(|millis| Duration::from_millis(millis.parse().unwrap()))
        .unwrap_or(quic_connection_parameters.finalize_timeout);

    quic_connection_parameters.connection_retry_count = env::var("QUIC_CONNECTION_RETRY_COUNT")
        .map(|millis| millis.parse().unwrap())
        .unwrap_or(quic_connection_parameters.connection_retry_count);

    quic_connection_parameters.max_number_of_connections =
        env::var("QUIC_MAX_NUMBER_OF_CONNECTIONS")
            .map(|millis| millis.parse().unwrap())
            .unwrap_or(quic_connection_parameters.max_number_of_connections);

    quic_connection_parameters.number_of_transactions_per_unistream =
        env::var("QUIC_NUMBER_OF_TRANSACTIONS_PER_TASK")
            .map(|millis| millis.parse().unwrap())
            .unwrap_or(quic_connection_parameters.number_of_transactions_per_unistream);

    quic_connection_parameters.unistreams_to_create_new_connection_in_percentage =
        env::var("QUIC_PERCENTAGE_TO_CREATE_NEW_CONNECTION")
            .map(|millis| millis.parse().unwrap())
            .unwrap_or(
                quic_connection_parameters.unistreams_to_create_new_connection_in_percentage,
            );

    Some(quic_connection_parameters)
}
