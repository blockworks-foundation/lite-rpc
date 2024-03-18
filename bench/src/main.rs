use bench::oldbench::TransactionSize;
use bench::{
    helpers::BenchHelper,
    metrics::{AvgMetric, Metric, TxMetricData},
    oldbench, Args,
};
use clap::Parser;
use dashmap::DashMap;
use futures::future::join_all;
use log::{error, info, warn};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Signature;
use solana_sdk::{
    commitment_config::CommitmentConfig, hash::Hash, signature::Keypair, signer::Signer,
    slot_history::Slot,
};
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
    tracing_subscriber::fmt::init();

    let Args {
        tx_count,
        runs,
        run_interval_ms,
        metrics_file_name,
        lite_rpc_addr,
        transaction_save_file,
        large_transactions,
    } = Args::parse();

    let cu_price_micro_lamports = 3;

    let mut run_interval_ms = tokio::time::interval(Duration::from_millis(run_interval_ms));

    let transaction_size = if large_transactions {
        TransactionSize::Large
    } else {
        TransactionSize::Small
    };

    info!("Connecting to LiteRPC using {lite_rpc_addr}");

    let mut avg_metric = AvgMetric::default();

    let mut tasks = vec![];

    let funded_payer = BenchHelper::get_payer().await.unwrap();
    info!("Payer: {}", funded_payer.pubkey());

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        lite_rpc_addr.clone(),
        CommitmentConfig::confirmed(),
    ));
    let bh = rpc_client.get_latest_blockhash().await.unwrap();
    let slot = rpc_client.get_slot().await.unwrap();
    let block_hash: Arc<RwLock<Hash>> = Arc::new(RwLock::new(bh));
    let current_slot = Arc::new(AtomicU64::new(slot));
    {
        // block hash updater task
        let block_hash = block_hash.clone();
        let rpc_client = rpc_client.clone();
        let current_slot = current_slot.clone();
        tokio::spawn(async move {
            loop {
                let bh = rpc_client.get_latest_blockhash().await;
                match bh {
                    Ok(bh) => {
                        let mut lock = block_hash.write().await;
                        *lock = bh;
                    }
                    Err(e) => println!("blockhash update error {}", e),
                }

                let slot = rpc_client.get_slot().await;
                match slot {
                    Ok(slot) => {
                        current_slot.store(slot, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(e) => println!("slot {}", e),
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };

    // transaction logger
    let (tx_log_sx, mut tx_log_rx) = tokio::sync::mpsc::unbounded_channel::<TxMetricData>();
    let log_transactions = !transaction_save_file.is_empty();
    if log_transactions {
        tokio::spawn(async move {
            let mut tx_writer = csv::Writer::from_path(transaction_save_file).unwrap();
            while let Some(x) = tx_log_rx.recv().await {
                tx_writer.serialize(x).unwrap();
            }
        });
    }

    for seed in 0..runs {
        let funded_payer = Keypair::from_bytes(funded_payer.to_bytes().as_slice()).unwrap();
        tasks.push(tokio::spawn(oldbench::bench(
            rpc_client.clone(),
            tx_count,
            funded_payer,
            seed as u64,
            block_hash.clone(),
            current_slot.clone(),
            tx_log_sx.clone(),
            log_transactions,
            transaction_size,
            cu_price_micro_lamports,
        )));
        // wait for an interval
        run_interval_ms.tick().await;
    }

    let join_res = join_all(tasks).await;

    let mut run_num = 1;

    let mut csv_writer = csv::Writer::from_path(metrics_file_name).unwrap();
    for res in join_res {
        match res {
            Ok(metric) => {
                info!("Run {run_num}: Sent and Confirmed {tx_count} tx(s) in {metric:?} with",);
                // update avg metric
                avg_metric += &metric;
                csv_writer.serialize(metric).unwrap();
            }
            Err(_) => {
                error!("join error for run {}", run_num);
            }
        }
        run_num += 1;
    }

    let avg_metric = Metric::from(avg_metric);

    info!("Avg Metric {avg_metric:?}",);
    csv_writer.serialize(avg_metric).unwrap();

    csv_writer.flush().unwrap();
}
