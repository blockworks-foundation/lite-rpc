use std::sync::Arc;

use bench::{
    cli::Args,
    helpers::BenchHelper,
    metrics::{AvgMetric, Metric},
};
use clap::Parser;
use futures::future::join_all;
use log::{error, info};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, signer::Signer};
use tokio::{
    sync::Mutex,
    time::{Duration, Instant},
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let Args {
        tx_count,
        runs,
        run_interval_ms,
        metrics_file_name,
        lite_rpc_addr,
    } = Args::parse();

    let mut run_interval_ms = tokio::time::interval(Duration::from_millis(run_interval_ms));

    info!("Connecting to {lite_rpc_addr}");

    let mut csv_writer = csv::Writer::from_path(metrics_file_name).unwrap();

    let mut avg_metric = AvgMetric::default();

    let mut tasks = vec![];

    for _ in 0..runs {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            lite_rpc_addr.clone(),
            CommitmentConfig::confirmed(),
        ));
        tasks.push(tokio::spawn(bench(rpc_client.clone(), tx_count)));
        // wait for an interval
        run_interval_ms.tick().await;
    }

    let join_res = join_all(tasks).await;

    let mut run_num = 1;
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

async fn bench(rpc_client: Arc<RpcClient>, tx_count: usize) -> Metric {
    let funded_payer = BenchHelper::get_payer().await.unwrap();

    println!("payer {}", funded_payer.pubkey());
    let blockhash = rpc_client.get_latest_blockhash().await.unwrap();

    let txs = BenchHelper::generate_txs(tx_count, &funded_payer, blockhash, None);

    let metric = Arc::new(Mutex::new(Metric::default()));
    let mut tx_sent_tasks = vec![];
    for tx in txs {
        let rpc_client = rpc_client.clone();
        let metric = metric.clone();
        let task = tokio::spawn(async move {
            let start_time = Instant::now();
            let signature = rpc_client.send_transaction(&tx).await.unwrap();
            let sent_duration = start_time.elapsed();
            let mut confirmed = false;
            while start_time.elapsed() < Duration::from_secs(60) {
                tokio::time::sleep(Duration::from_millis(1)).await;
                if let Ok(b) = rpc_client.confirm_transaction(&signature).await {
                    if b == true {
                        confirmed = true;
                        break;
                    }
                }
            }
            let send_and_confirm_time = start_time.elapsed();
            let mut metric = metric.lock().await;
            if confirmed {
                metric.add_successful_transaction(sent_duration, send_and_confirm_time);
            } else {
                metric.add_unsuccessful_transaction();
            }
        });
        tx_sent_tasks.push(task);
    }
    futures::future::join_all(tx_sent_tasks).await;
    let m = *metric.lock().await;
    m
}
