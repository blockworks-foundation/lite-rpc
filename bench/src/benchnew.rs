use std::path::PathBuf;
use std::time::Duration;

use bench::{
    benches::{
        api_load::api_load, confirmation_rate::confirmation_rate,
        confirmation_slot::confirmation_slot,
    },
    metrics::{PingThing, PingThingCluster},
    tx_size::TxSize,
    BenchmarkTransactionParams,
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
        test_duration_ms: u64,
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
        /// Set websocket source (blockSubscribe method) for transaction status updates.
        /// You might want to send tx to one RPC and listen to another (reliable) RPC for status updates.
        /// Not all RPC nodes support this method.
        /// If not provided, the RPC URL is used to derive the websocket URL.
        #[clap(short = 'w', long)]
        tx_status_websocket_addr: Option<String>,
        #[clap(short, long)]
        size_tx: TxSize,
        /// Maximum confirmation time in milliseconds. After this, the txn is considered unconfirmed
        #[clap(short, long, default_value_t = 15_000)]
        max_timeout_ms: u64,
        #[clap(short, long)]
        txs_per_run: usize,
        #[clap(short, long)]
        num_of_runs: usize,
        /// The CU price in micro lamports
        #[clap(short, long, default_value_t = 300)]
        #[arg(short = 'f')]
        cu_price: u64,
    },
    /// Compares the confirmation slot of txs sent to 2 different RPCs
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
        #[clap(long)]
        tx_status_websocket_addr_a: Option<String>,
        #[clap(long)]
        tx_status_websocket_addr_b: Option<String>,
        #[clap(short, long)]
        size_tx: TxSize,
        /// Maximum confirmation time in milliseconds. After this, the txn is considered unconfirmed
        #[clap(short, long, default_value_t = 15_000)]
        max_timeout_ms: u64,
        #[clap(short, long)]
        num_of_runs: usize,
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
            test_duration_ms,
            cu_price,
        } => {
            api_load(&payer_path, rpc_url, test_duration_ms, cu_price)
                .await
                .unwrap();
        }
        SubCommand::ConfirmationRate {
            payer_path,
            rpc_url,
            tx_status_websocket_addr,
            size_tx,
            max_timeout_ms,
            txs_per_run,
            num_of_runs,
            cu_price,
        } => confirmation_rate(
            &payer_path,
            rpc_url,
            tx_status_websocket_addr,
            BenchmarkTransactionParams {
                tx_size: size_tx,
                cu_price_micro_lamports: cu_price,
            },
            Duration::from_millis(max_timeout_ms),
            txs_per_run,
            num_of_runs,
        )
        .await
        .unwrap(),
        SubCommand::ConfirmationSlot {
            payer_path,
            rpc_a,
            rpc_b,
            tx_status_websocket_addr_a,
            tx_status_websocket_addr_b,
            size_tx,
            max_timeout_ms,
            num_of_runs,
            cu_price,
            ping_thing_token,
        } => confirmation_slot(
            &payer_path,
            rpc_a,
            rpc_b,
            tx_status_websocket_addr_a,
            tx_status_websocket_addr_b,
            BenchmarkTransactionParams {
                tx_size: size_tx,
                cu_price_micro_lamports: cu_price,
            },
            Duration::from_millis(max_timeout_ms),
            num_of_runs,
            ping_thing_token.map(|t| PingThing {
                cluster: PingThingCluster::Mainnet,
                va_api_key: t,
            }),
        )
        .await
        .unwrap(),
    }
}
