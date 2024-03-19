use std::path::PathBuf;

use bench::{
    benches::{
        api_load::api_load, confirmation_rate::confirmation_rate,
        confirmation_slot::confirmation_slot,
    },
    tx_size::TxSize,
};
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[clap(version, about)]

struct Arguments {
    #[clap(subcommand)]
    subcommand: SubCommand,
}

#[derive(Subcommand, Debug)]
enum SubCommand {
    ApiLoad {
        #[clap(short, long)]
        payer_path: PathBuf,
        #[clap(short, long)]
        rpc_url: String,
        #[clap(short, long)]
        time_ms: u64,
        /// The CU price in micro lamports
        #[clap(short, long, default_value_t = 3)]
        #[arg(short = 'f')]
        cu_price: u64,
    },
    ConfirmationRate {
        #[clap(short, long)]
        payer_path: PathBuf,
        #[clap(short, long)]
        rpc_url: String,
        #[clap(short, long)]
        size_tx: TxSize,
        #[clap(short, long)]
        txns_per_round: usize,
        #[clap(short, long)]
        num_rounds: usize,
        /// The CU price in micro lamports
        #[clap(short, long, default_value_t = 300)]
        #[arg(short = 'f')]
        cu_price: u64,
    },
    /// Compares the confirmation slot of txns sent to 2 different RPCs
    ConfirmationSlot {
        #[clap(short, long)]
        payer_path: PathBuf,
        /// URL of the 1st RPC
        #[clap(short, long)]
        #[arg(short = 'a')]
        rpc_a: String,
        /// URL of the 2nd RPC
        #[clap(short, long)]
        #[arg(short = 'b')]
        rpc_b: String,
        #[clap(short, long)]
        size_tx: TxSize,
        /// Maximum confirmation time in milliseconds. After this, the txn is considered unconfirmed
        #[clap(short, long, default_value_t = 15_000)]
        max_timeout_ms: u64,
        #[clap(short, long)]
        num_rounds: usize,
        /// The CU price in micro lamports
        #[clap(short, long, default_value_t = 300)]
        #[arg(short = 'f')]
        cu_price: u64,
        #[clap(long)]
        ping_thing_token: Option<String>,
    },
}

pub fn initialize_logger() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_thread_ids(true)
        .with_line_number(true)
        .init();
}

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    let args = Arguments::parse();
    initialize_logger();

    match args.subcommand {
        SubCommand::ApiLoad {
            payer_path,
            rpc_url,
            time_ms,
            cu_price,
        } => {
            api_load(&payer_path, rpc_url, time_ms, cu_price)
                .await
                .unwrap();
        }
        SubCommand::ConfirmationRate {
            payer_path,
            rpc_url,
            size_tx,
            txns_per_round,
            num_rounds,
            cu_price,
        } => confirmation_rate(
            &payer_path,
            rpc_url,
            size_tx,
            txns_per_round,
            num_rounds,
            cu_price,
        )
        .await
        .unwrap(),
        SubCommand::ConfirmationSlot {
            payer_path,
            rpc_a,
            rpc_b,
            size_tx,
            max_timeout_ms,
            num_rounds,
            cu_price,
            ping_thing_token,
        } => confirmation_slot(
            &payer_path,
            rpc_a,
            rpc_b,
            size_tx,
            max_timeout_ms,
            num_rounds,
            cu_price,
            ping_thing_token,
        )
        .await
        .unwrap(),
    }
}
