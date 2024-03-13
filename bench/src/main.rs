use bench::{
    benches::{api_load::api_load, confirmation_rate::confirmation_rate, confirmation_slot::confirmation_slot},
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
        payer_path: String,
        #[clap(short, long)]
        rpc_url: String,
        #[clap(short, long)]
        time_ms: u64,
    },
    ConfirmationRate {
        #[clap(short, long)]
        payer_path: String,
        #[clap(short, long)]
        rpc_url: String,
        #[clap(short, long)]
        tx_size: TxSize,
        #[clap(short, long)]
        num_txns: usize,
        #[clap(short, long)]
        num_runs: usize,
    },
    ConfirmationSlot {
        #[clap(short, long)]
        payer_path: String,
        #[clap(short, long)]
        rpc_a: String,
        #[clap(short, long)]
        rpc_b: String,
        #[clap(short, long)]
        tx_size: TxSize,
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
        } => {
            api_load(payer_path, rpc_url, time_ms).await.unwrap();
        }
        SubCommand::ConfirmationRate {
            payer_path,
            rpc_url,
            tx_size,
            num_txns,
            num_runs,
        } => confirmation_rate(payer_path, rpc_url, tx_size, num_txns, num_runs)
            .await
            .unwrap(),
        SubCommand::ConfirmationSlot {
            payer_path,
            rpc_a,
            rpc_b,
            tx_size
        } => confirmation_slot(payer_path, rpc_a, rpc_b, tx_size).await.unwrap(),
    }
}
