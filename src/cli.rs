use crate::{DEFAULT_RPC_ADDR, DEFAULT_WS_ADDR};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value_t = String::from(DEFAULT_RPC_ADDR))]
    pub rpc_addr: String,
    #[arg(short, long, default_value_t = String::from(DEFAULT_WS_ADDR))]
    pub ws_addr: String,
    #[arg(short, long, default_value_t = String::from("127.0.0.1:8890"))]
    pub lite_rpc_addr: String,
    #[arg(short, long, default_value_t = false)]
    pub batch_transactions: bool,
}
