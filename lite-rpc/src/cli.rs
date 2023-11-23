use crate::{
    DEFAULT_FANOUT_SIZE, DEFAULT_GRPC_ADDR, DEFAULT_RETRY_TIMEOUT, DEFAULT_RPC_ADDR,
    DEFAULT_WS_ADDR, MAX_RETRIES,
};
use config_parser::ConfigParser;

#[derive(ConfigParser, Clone)]
pub struct Args {
    #[arg(short, long, default = String::from("config.json"))]
    pub config: String,
    #[arg(short, long, default = String::from(DEFAULT_RPC_ADDR))]
    pub rpc_addr: String,
    #[arg(short, long, default = String::from(DEFAULT_WS_ADDR))]
    pub ws_addr: String,
    #[arg(short = 'l', long, default = String::from("[::]:8890"))]
    pub lite_rpc_http_addr: String,
    #[arg(short = 's', long, default = String::from("[::]:8891"))]
    pub lite_rpc_ws_addr: String,
    #[arg(short = 'f', long, default = DEFAULT_FANOUT_SIZE, desc="tpu fanout") ]
    pub fanout_size: u64,
    #[arg(short = 'p', long, desc = "enable logging to postgres")]
    pub enable_postgres: bool,
    #[arg(short = 'm', long, default = String::from("[::]:9091"), desc = "enable metrics to prometheus at addr")]
    pub prometheus_addr: String,
    #[arg(short = 'k', long, default = String::new())]
    pub identity_keypair: String,
    #[arg(long, default = MAX_RETRIES)]
    pub maximum_retries_per_tx: usize,
    #[arg(long, default = DEFAULT_RETRY_TIMEOUT)]
    pub transaction_retry_after_secs: u64,
    #[arg(long)]
    pub quic_proxy_addr: Option<String>,
    #[arg(short = 'g', long)]
    pub use_grpc: bool,
    #[arg(long, default = String::from(DEFAULT_GRPC_ADDR), desc = "grpc address")]
    pub grpc_addr: String,
    #[arg(long)]
    pub grpc_x_token: Option<String>,
}
