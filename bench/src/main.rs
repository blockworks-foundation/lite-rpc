use bench::{
    cli::Args,
    helpers::BenchHelper,
    metrics::{AvgMetric, Metric, TxMetricData},
    sync::SyncService,
    tx_logger::TxLogger,
    TransactionSize,
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
    task::JoinHandle,
    time::{Duration, Instant},
};

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> anyhow::Result<()> {
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

    let transaction_size = if large_transactions {
        TransactionSize::Large
    } else {
        TransactionSize::Small
    };

    let funded_payer = Arc::new(BenchHelper::get_payer().await.unwrap());

    info!("Connecting to LiteRPC using {lite_rpc_addr}");
    info!("Payer: {}", funded_payer.pubkey());

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        lite_rpc_addr.clone(),
        CommitmentConfig::confirmed(),
    ));

    let sync_service = SyncService::new(&rpc_client).await?;
    tokio::spawn(sync_service.clone().sync(rpc_client.clone()));

    // transaction logger
    let tx_log_channel = if !transaction_save_file.is_empty() {
        let (tx_log_sx, tx_log_rx) = tokio::sync::mpsc::unbounded_channel::<TxMetricData>();
        tokio::spawn(TxLogger::log(&transaction_save_file, tx_log_rx));
        Some(tx_log_sx)
    } else {
        None
    };

    // closure to create bench join handle
    let spawn_bench = |seed: u64| -> JoinHandle<Metric> {
        tokio::spawn(bench(
            rpc_client.clone(),
            tx_count,
            funded_payer.clone(),
            seed as u64,
            sync_service.clone(),
            tx_log_channel,
            transaction_size,
        ))
    };

    // if 0 run things in parallel
    let metrics = if run_interval_ms == 0 {
        join_all((0..runs).map(spawn_bench)).await
    } else {
        let metrics = Vec::with_capacity(runs as usize);

        // run things in sequence
        for seed in 0..runs {
            let metric = spawn_bench(seed).await;
            metrics.push(metric);

            tokio::time::sleep(Duration::from_millis(run_interval_ms)).await;
        }

        metrics
    };

    let mut avg_metric = AvgMetric::default();

    let mut csv_writer = csv::Writer::from_path(metrics_file_name).unwrap();

    for (index, res) in metrics.into_iter().enumerate() {
        let run_num = index + 1;

        match res {
            Ok(metric) => {
                info!("Run {run_num}: Sent and Confirmed {tx_count} tx(s) in {metric:?} with",);
                // update avg metric
                avg_metric += &metric;
                csv_writer.serialize(metric).unwrap();
            }
            Err(_) => {
                error!("join error for run {run_num}");
            }
        }
    }

    let avg_metric = Metric::from(avg_metric);

    info!("Avg Metric {avg_metric:?}",);
    csv_writer.serialize(avg_metric).unwrap();

    csv_writer.flush().unwrap();

    Ok(())
}

#[derive(Clone, Debug, Copy)]
struct TxSendData {
    sent_instant: Instant,
    sent_slot: Slot,
    transaction_bytes: u64,
}

struct ApiCallerResult {
    gross_send_time: Duration,
}

async fn bench(
    rpc_client: Arc<RpcClient>,
    tx_count: usize,
    funded_payer: Arc<Keypair>,
    seed: u64,
    sync_service: SyncService,
    tx_metric_sx: Option<UnboundedSender<TxMetricData>>,
    transaction_size: TransactionSize,
) -> Metric {
    // transaction sender task
    let n_chars = match transaction_size {
        TransactionSize::Small => 10,
        TransactionSize::Large => 240, // 565 is max but we need to lower that to not burn the CUs
    };

    let slot = rpc_client.get_slot().await.unwrap();
    let block_hash = rpc_client.get_latest_blockhash().await.unwrap();

    let txs = BenchHelper::generate_random_strings(tx_count, Some(seed), n_chars)
        .into_iter()
        .map(|rand_str| {
            let tx = match transaction_size {
                TransactionSize::Small => {
                    BenchHelper::create_memo_tx_small(&rand_str, &funded_payer, block_hash)
                }
                TransactionSize::Large => {
                    BenchHelper::create_memo_tx_large(&rand_str, &funded_payer, block_hash)
                }
            };

            let transaction_bytes = bincode::serialized_size(&tx).unwrap();

            (
                tx.signatures[0],
                TxSendData {
                    sent_instant: Instant::now(),
                    transaction_bytes,
                },
            )
        })
        .collect::<Vec<_>>();

    let tx_promises = txs
        .iter()
        .map(|tx| rpc_client.send_transaction(&tx.0))
        .collect::<Vec<_>>();

    let tx_send_time = Instant::now();
    join_all(tx_promises).await;
    let tx_send_duration = tx_send_time.elapsed();

    // confirm transactions

    let mut metric = Metric::default();
    let mut confirmed_count = 0;

    // confirm only for 60 seconds
    let confirmation_time = Instant::now();

    while (confirmed_count < tx_count) || (confirmation_time.elapsed() < Duration::from_secs(60)) {
        // get signature status to get the slot of the transaction. as close as to confirmed slot
        // but not exact
        let status_futures = txs
            .iter()
            .map(|(sig, _)| rpc_client.get_signature_statuses(&[*sig]));

        let statuses = join_all(status_futures).await;

        for (index, status) in statuses.into_iter().enumerate() {
            let (sig, tx_data) = &txs[index];

            let Ok(status) = status else {
                warn!("failed to get signature status {}", sig.to_string());
                continue;
            };

            confirmed_count += 1;

            let time_to_confirm = tx_data.sent_instant.elapsed();
            let transaction_bytes = tx_data.transaction_bytes;

            metric.add_successful_transaction(
                tx_data.sent_duration,
                time_to_confirm,
                transaction_bytes,
            );

            let Some(tx_metric_sx) = &tx_metric_sx else {
                continue;
            };

            let _ = tx_metric_sx.send(TxMetricData {
                signature: sig.to_string(),
                sent_slot: tx_data.sent_slot,
                confirmed_slot: status.context.slot,
                time_to_send_in_millis: tx_data.sent_duration.as_millis() as u64,
                time_to_confirm_in_millis: time_to_confirm.as_millis() as u64,
            });
        }

        // sleep for a ms
        tokio::time::sleep(Duration::from_millis(1)).await;
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
