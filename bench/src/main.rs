use bench::{
    cli::Args,
    helpers::BenchHelper,
    metrics::{AvgMetric, Metric},
};
use clap::Parser;
use dashmap::DashMap;
use futures::future::join_all;
use log::{error, info};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig, hash::Hash, signature::Keypair, signer::Signer,
};
use std::sync::Arc;
use tokio::{
    sync::RwLock,
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
    let block_hash: Arc<RwLock<Hash>> = Default::default();

    let funded_payer = BenchHelper::get_payer().await.unwrap();
    println!("payer : {}", funded_payer.pubkey());

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        lite_rpc_addr.clone(),
        CommitmentConfig::confirmed(),
    ));

    {
        // block hash updater task
        let block_hash = block_hash.clone();
        let rpc_client = rpc_client.clone();
        tokio::spawn(async move {
            loop {
                let bh = rpc_client.get_latest_blockhash().await;
                if let Ok(bh) = bh {
                    let mut lock = block_hash.write().await;
                    *lock = bh;
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }

    for seed in 0..runs {
        let funded_payer = Keypair::from_bytes(funded_payer.to_bytes().as_slice()).unwrap();
        tasks.push(tokio::spawn(bench(
            rpc_client.clone(),
            tx_count,
            funded_payer,
            seed as u64,
            block_hash.clone(),
        )));
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

#[derive(Clone, Debug, Copy)]
struct TxSendData {
    sent_duration: Duration,
    sent_instant: Instant,
}

async fn bench(
    rpc_client: Arc<RpcClient>,
    tx_count: usize,
    funded_payer: Keypair,
    seed: u64,
    block_hash: Arc<RwLock<Hash>>,
) -> Metric {
    let map_of_txs = Arc::new(DashMap::new());

    // transaction sender task
    {
        let map_of_txs = map_of_txs.clone();
        let rpc_client = rpc_client.clone();
        tokio::spawn(async move {
            let map_of_txs = map_of_txs.clone();
            let rand_strings = BenchHelper::generate_random_strings(tx_count, Some(seed));

            for rand_string in rand_strings {
                let blockhash = *block_hash.read().await;
                let tx = BenchHelper::create_memo_tx(&rand_string, &funded_payer, blockhash);
                let start_time = Instant::now();
                if let Ok(signature) = rpc_client.send_transaction(&tx).await {
                    map_of_txs.insert(
                        signature,
                        TxSendData {
                            sent_duration: start_time.elapsed(),
                            sent_instant: Instant::now(),
                        },
                    );
                }
            }
        });
    }

    let mut metric = Metric::default();
    let confirmation_time = Instant::now();
    let mut confirmed_count = 0;
    while confirmation_time.elapsed() < Duration::from_secs(60)
        && !(map_of_txs.is_empty() && confirmed_count == tx_count)
    {
        let signatures = map_of_txs
            .iter()
            .map(|x| x.key().clone())
            .collect::<Vec<_>>();
        if let Ok(res) = rpc_client.get_signature_statuses(&signatures).await {
            for i in 0..signatures.len() {
                let tx_status = &res.value[i];
                if let Some(_) = tx_status {
                    let signature = signatures[i];
                    let tx_data = map_of_txs.get(&signature).unwrap();
                    metric.add_successful_transaction(
                        tx_data.sent_duration,
                        tx_data.sent_instant.elapsed(),
                    );
                    map_of_txs.remove(&signature);
                    confirmed_count += 1;
                }
            }
        }
    }

    for tx in map_of_txs.iter() {
        metric.add_unsuccessful_transaction(tx.sent_duration);
    }
    metric.finalize();
    metric
}
