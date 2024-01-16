use bench::strategies::Strategy;
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
    dotenv::dotenv().ok();

    tracing_subscriber::fmt::init();

    let Args {
        metrics_file_name,
        strategy,
    } = Args::parse();

    let res = strategy.execute().await.unwrap();

    if res.is_empty() {
        return;
    }

    let mut csv_writer = csv::Writer::from_path(&metrics_file_name).unwrap();

    let headers = res.first().unwrap().as_object().unwrap().keys().collect::<Vec<_>>();
    csv_writer.write_record(&headers).unwrap();

    res.iter().for_each(|x| {
        let row = x.as_object().unwrap().values().map(|v| v.to_string()).collect::<Vec<_>>();
        csv_writer.write_record(&row).unwrap();
    });

    csv_writer.flush().unwrap();
}

#[derive(Clone, Debug, Copy)]
struct TxSendData {
    sent_duration: Duration,
    sent_instant: Instant,
    sent_slot: Slot,
    transaction_bytes: u64,
}

struct ApiCallerResult {
    gross_send_time: Duration,
}

#[allow(clippy::too_many_arguments)]
async fn bench(
    rpc_client: Arc<RpcClient>,
    tx_count: usize,
    funded_payer: Keypair,
    seed: Option<u64>,
    block_hash: Arc<RwLock<Hash>>,
    current_slot: Arc<AtomicU64>,
    tx_metric_sx: UnboundedSender<TxMetricData>,
    log_txs: bool,
    tx_size: TxSize,
) -> Metric {
    let map_of_txs: Arc<DashMap<Signature, TxSendData>> = Arc::new(DashMap::new());
    // transaction sender task
    let api_caller_result = {
        let map_of_txs = map_of_txs.clone();
        let rpc_client = rpc_client.clone();
        let current_slot = current_slot.clone();

        tokio::spawn(async move {
            let map_of_txs = map_of_txs.clone();

            let n_chars = match tx_size {
                TxSize::Small => 10,
                TxSize::Large => 240, // 565 is max but we need to lower that to not burn the CUs
            };

            let mut rng = BenchHelper::create_rng(seed);
            let rand_strings = BenchHelper::generate_random_strings(&mut rng, tx_count, n_chars);

            let bench_start_time = Instant::now();

            for rand_string in &rand_strings {
                let blockhash = { *block_hash.read().await };
                let tx = match tx_size {
                    TxSize::Small => {
                        BenchHelper::create_memo_tx_small(rand_string, &funded_payer, blockhash)
                    }
                    TxSize::Large => {
                        BenchHelper::create_memo_tx_large(rand_string, &funded_payer, blockhash)
                    }
                };
                let start_time = Instant::now();
                match rpc_client.send_transaction(&tx).await {
                    Ok(signature) => {
                        map_of_txs.insert(
                            signature,
                            TxSendData {
                                sent_duration: start_time.elapsed(),
                                sent_instant: Instant::now(),
                                sent_slot: current_slot.load(std::sync::atomic::Ordering::Relaxed),
                                transaction_bytes: bincode::serialized_size(&tx).unwrap(),
                            },
                        );
                    }
                    Err(e) => {
                        log::warn!("tx send failed with error {}", e);
                    }
                }
            }
            ApiCallerResult {
                gross_send_time: bench_start_time.elapsed(),
            }
        })
    };

    let mut metric = Metric::default();
    let confirmation_time = Instant::now();
    let mut confirmed_count = 0;
    while confirmation_time.elapsed() < Duration::from_secs(60)
        && !(map_of_txs.is_empty() && confirmed_count == tx_count)
    {
        let signatures = map_of_txs.iter().map(|x| *x.key()).collect::<Vec<_>>();
        if signatures.is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
            continue;
        }

        if let Ok(res) = rpc_client.get_signature_statuses(&signatures).await {
            for (i, signature) in signatures.iter().enumerate() {
                let tx_status = &res.value[i];
                if tx_status.is_some() {
                    let tx_data = map_of_txs.get(signature).unwrap();
                    let time_to_confirm = tx_data.sent_instant.elapsed();
                    let transaction_bytes = tx_data.transaction_bytes;
                    metric.add_successful_transaction(
                        tx_data.sent_duration,
                        time_to_confirm,
                        transaction_bytes,
                    );

                    if log_txs {
                        let _ = tx_metric_sx.send(TxMetricData {
                            signature: signature.to_string(),
                            sent_slot: tx_data.sent_slot,
                            confirmed_slot: current_slot.load(Ordering::Relaxed),
                            time_to_send_in_millis: tx_data.sent_duration.as_millis() as u64,
                            time_to_confirm_in_millis: time_to_confirm.as_millis() as u64,
                        });
                    }
                    drop(tx_data);
                    map_of_txs.remove(signature);
                    confirmed_count += 1;
                }
            }
        }
    }

    for tx in map_of_txs.iter() {
        metric.add_unsuccessful_transaction(tx.sent_duration, tx.transaction_bytes);
    }

    let api_caller_result = api_caller_result
        .await
        .expect("api caller task must succeed");

    metric
        .set_total_gross_send_time(api_caller_result.gross_send_time.as_micros() as f64 / 1_000.0);

    metric.finalize();
    metric
}
