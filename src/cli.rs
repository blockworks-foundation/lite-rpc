use crate::{
    DEFAULT_CLEAN_INTERVAL_MS, DEFAULT_FANOUT_SIZE, DEFAULT_RPC_ADDR, DEFAULT_TX_BATCH_INTERVAL_MS,
    DEFAULT_TX_BATCH_SIZE, DEFAULT_WS_ADDR,
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
    /// batch size of each batch forward
    #[arg(short = 'b', long, default_value_t = DEFAULT_TX_BATCH_SIZE)]
    pub tx_batch_size: usize,
    /// tpu fanout
    #[arg(short = 'f', long, default_value_t = DEFAULT_FANOUT_SIZE) ]
    pub fanout_size: u64,
    /// interval between each batch forward
    #[arg(short = 'i', long, default_value_t = DEFAULT_TX_BATCH_INTERVAL_MS)]
    pub tx_batch_interval_ms: u64,
    /// interval between clean
    #[arg(short = 'c', long, default_value_t = DEFAULT_CLEAN_INTERVAL_MS)]
    pub clean_interval_ms: u64,
    /// addr to postgres
    #[arg(short = 'p', long)]
    pub enable_postgres: bool 
}
