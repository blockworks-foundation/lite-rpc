use bench::{
    cli::Args,
    helpers::BenchHelper,
    metrics::{Metric, TxMetricData},
    tx_size::TxSize,
};
use clap::Parser;
use dashmap::DashMap;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Signature;
use solana_sdk::{hash::Hash, signature::Keypair, slot_history::Slot};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::{
    sync::{mpsc::UnboundedSender, RwLock},
    time::{Duration, Instant},
};

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO).init();

    let Args {
        metrics_file_name,
        strategy,
    } = Args::parse();

    strategy.execute(&metrics_file_name).await.unwrap();
}
