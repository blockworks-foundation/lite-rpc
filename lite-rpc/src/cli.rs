use crate::{
    DEFAULT_CLEAN_INTERVAL_MS, DEFAULT_FANOUT_SIZE, DEFAULT_RETRY_TIMEOUT, DEFAULT_RPC_ADDR,
    DEFAULT_WS_ADDR, MAX_RETRIES,
};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value_t = String::from(DEFAULT_RPC_ADDR))]
    pub rpc_addr: String,
    #[arg(short, long, default_value_t = String::from(DEFAULT_WS_ADDR))]
    pub ws_addr: String,
    #[arg(short = 'l', long, default_value_t = String::from("[::]:8890"))]
    pub lite_rpc_http_addr: String,
    #[arg(short = 's', long, default_value_t = String::from("[::]:8891"))]
    pub lite_rpc_ws_addr: String,
    /// tpu fanout
    #[arg(short = 'f', long, default_value_t = DEFAULT_FANOUT_SIZE) ]
    pub fanout_size: u64,
    /// interval between clean
    #[arg(short = 'c', long, default_value_t = DEFAULT_CLEAN_INTERVAL_MS)]
    pub clean_interval_ms: u64,
    /// enable logging to postgres
    #[arg(short = 'p', long)]
    pub enable_postgres: bool,
    /// enable metrics to prometheus at addr
    #[arg(short = 'm', long, default_value_t = String::from("[::]:9091"))]
    pub prometheus_addr: String,
    #[arg(short = 'k', long, default_value_t = String::new())]
    pub identity_keypair: String,
    #[arg(long, default_value_t = MAX_RETRIES)]
    pub maximum_retries_per_tx: usize,
    #[arg(long, default_value_t = DEFAULT_RETRY_TIMEOUT)]
    pub transaction_retry_after_secs: u64,
}
