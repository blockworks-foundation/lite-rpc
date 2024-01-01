use crate::{helpers::USER_KEYPAIR_PATH, strategies::Strategies, tx_size::TxSize};
use clap::{command, Parser};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Metrics output file name
    #[arg(short = 'm', long, default_value_t = String::from("metrics.csv"))]
    pub metrics_file_name: String,
    /// strategy for sending transactions
    #[command(subcommand)]
    pub strategy: Strategies,
}

#[derive(clap::Args, Debug)]
pub struct RpcArgs {
    #[arg(short = 'r', long, default_value_t = String::from("http://0.0.0.0:8899"))]
    pub rpc_addr: String,
    #[arg(short = 'p', long, default_value_t = USER_KEYPAIR_PATH.to_string())]
    pub payer: String,
    /// choose between small (179 bytes) and large (1186 bytes) transactions
    #[arg(short = 't', long, value_enum, default_value_t = TxSize::Small)]
    pub tx_size: TxSize,
}

#[derive(clap::Args, Debug)]
pub struct LiteRpcArgs {
    #[arg(short = 'l', long, default_value_t = String::from("http://0.0.0.0:8890"))]
    pub lite_rpc_addr: String,
}
