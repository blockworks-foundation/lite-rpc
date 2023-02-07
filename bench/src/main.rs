use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use bench::{
    cli::Args,
    helpers::BenchHelper,
    metrics::{AvgMetric, Metric},
};
use clap::Parser;
use log::info;
use solana_rpc_client::{nonblocking::rpc_client::RpcClient, rpc_client::SerializableTransaction};
use solana_sdk::{commitment_config::CommitmentConfig, signature::Signature};

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

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        lite_rpc_addr,
        CommitmentConfig::confirmed(),
    ));

    let mut csv_writer = csv::Writer::from_path(metrics_file_name).unwrap();

    let mut avg_metric = AvgMetric::default();

    for run_num in 0..runs {
        let metric = bench(rpc_client.clone(), tx_count).await;
        info!("Run {run_num}: Sent and Confirmed {tx_count} tx(s) in {metric:?} with",);
        // update avg metric
        avg_metric += &metric;
        // write metric to file
        csv_writer.serialize(metric).unwrap();
        // wait for an interval
        run_interval_ms.tick().await;
    }

    let avg_metric = Metric::from(avg_metric);

    info!("Avg Metric {avg_metric:?}",);
    csv_writer.serialize(avg_metric).unwrap();

    csv_writer.flush().unwrap();
}

async fn bench(rpc_client: Arc<RpcClient>, tx_count: usize) -> Metric {
    let funded_payer = BenchHelper::get_payer().await.unwrap();
    let blockhash = rpc_client.get_latest_blockhash().await.unwrap();

    let txs = BenchHelper::generate_txs(tx_count, &funded_payer, blockhash);

    let mut un_confirmed_txs: HashMap<Signature, Option<Instant>> =
        HashMap::with_capacity(txs.len());

    for tx in &txs {
        un_confirmed_txs.insert(*tx.get_signature(), None);
    }

    let start_time = Instant::now();

    info!("Sending and Confirming {tx_count} tx(s)",);

    let send_fut = {
        let rpc_client = rpc_client.clone();

        tokio::spawn(async move {
            for tx in txs {
                rpc_client.send_transaction(&tx).await.unwrap();
                info!("Tx {}", &tx.signatures[0]);
            }
            info!("Sent {tx_count} tx(s)");

            start_time.elapsed()
        })
    };

    let confirm_fut = tokio::spawn(async move {
        let mut metrics = Metric::default();

        while !un_confirmed_txs.is_empty() {
            let mut to_remove_txs = Vec::new();

            for (sig, time_elapsed_since_last_confirmed) in un_confirmed_txs.iter_mut() {
                let sig = *sig;

                if time_elapsed_since_last_confirmed.is_none() {
                    *time_elapsed_since_last_confirmed = Some(Instant::now())
                }

                if rpc_client.confirm_transaction(&sig).await.unwrap() {
                    metrics.txs_confirmed += 1;
                    to_remove_txs.push(sig);
                } else if time_elapsed_since_last_confirmed.unwrap().elapsed()
                    > Duration::from_secs(3)
                {
                    metrics.txs_un_confirmed += 1;
                    to_remove_txs.push(sig);
                }
            }

            for to_remove_tx in to_remove_txs {
                un_confirmed_txs.remove(&to_remove_tx);
            }
        }

        metrics.total_time_elapsed_sec = start_time.elapsed().as_secs_f64();
        metrics.txs_sent = tx_count as u64;

        metrics
    });

    let (send_fut, confirm_fut) = tokio::join!(send_fut, confirm_fut);
    let time_to_send_txs = send_fut.unwrap();
    let mut metrics = confirm_fut.unwrap();
    metrics.time_to_send_txs = time_to_send_txs.as_secs_f64();
    metrics.calc_tps();

    metrics
}
