use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use bench_utils::{
    helpers::BenchHelper,
    metrics::{AvgMetric, Metric},
};
use log::info;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_client::SerializableTransaction};
use solana_sdk::{
    commitment_config::CommitmentConfig, native_token::LAMPORTS_PER_SOL, signature::Signature,
};

use lite_client::{LiteClient, LOCAL_LIGHT_RPC_ADDR};
use simplelog::*;

const NUM_OF_TXS: usize = 20_000;
const NUM_OF_RUNS: usize = 1;
const CSV_FILE_NAME: &str = "metrics.csv";
const USE_SIGNATURE_STATUS_TO_CONFIRM: bool = false;

#[tokio::main]
async fn main() {
    TermLogger::init(
        LevelFilter::Info,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
    .unwrap();

    let lite_client = Arc::new(LiteClient(RpcClient::new_with_commitment(
        LOCAL_LIGHT_RPC_ADDR.to_string(),
        CommitmentConfig::confirmed(),
    )));

    let bench_helper = BenchHelper {
        lite_client,
        use_signature_status_to_confirm: USE_SIGNATURE_STATUS_TO_CONFIRM,
    };

    let mut csv_writer = csv::Writer::from_path(CSV_FILE_NAME).unwrap();

    let mut avg_metric = AvgMetric::default();

    for run_num in 0..NUM_OF_RUNS {
        let metric = bench(&bench_helper).await;
        info!("Run {run_num}: Sent and Confirmed {NUM_OF_TXS} tx(s) in {metric:?} with USE_SIGNATURE_STATUS_TO_CONFIRM {USE_SIGNATURE_STATUS_TO_CONFIRM}",);
        avg_metric += &metric;
        csv_writer.serialize(metric).unwrap();
    }

    let avg_metric = Metric::from(avg_metric);

    info!("Avg Metric {avg_metric:?}",);
    csv_writer.serialize(avg_metric).unwrap();

    csv_writer.flush().unwrap();
}

async fn bench(bench_helper: &BenchHelper) -> Metric {
    let funded_payer = bench_helper
        .new_funded_payer(LAMPORTS_PER_SOL * 2000)
        .await
        .unwrap();

    let txs = bench_helper
        .generate_txs(NUM_OF_TXS, &funded_payer)
        .await
        .unwrap();

    let mut un_confirmed_txs: HashMap<Signature, Option<Instant>> =
        HashMap::with_capacity(txs.len());

    for tx in &txs {
        un_confirmed_txs.insert(*tx.get_signature(), None);
    }

    let start_time = Instant::now();

    info!(
        "Sending and Confirming {NUM_OF_TXS} tx(s) with signature status polling {}",
        bench_helper.use_signature_status_to_confirm
    );

    let lite_client = bench_helper.lite_client.clone();

    let send_fut = {
        let lite_client = lite_client.clone();

        tokio::spawn(async move {
            for tx in txs {
                lite_client.send_transaction(&tx).await.unwrap();
                info!("Tx {}", &tx.signatures[0]);
            }
            info!("Sent {NUM_OF_TXS} tx(s)");

            start_time.elapsed()
        })
    };

    let use_signature_status_to_confirm = bench_helper.use_signature_status_to_confirm;
    let confirm_fut = tokio::spawn(async move {
        let mut metrics = Metric::default();

        while !un_confirmed_txs.is_empty() {
            let mut to_remove_txs = Vec::new();

            for (sig, time_elapsed_since_last_confirmed) in un_confirmed_txs.iter_mut() {
                let sig = *sig;

                if time_elapsed_since_last_confirmed.is_none() {
                    *time_elapsed_since_last_confirmed = Some(Instant::now())
                }

                let confirmed = if use_signature_status_to_confirm {
                    if let Some(err) = lite_client.get_signature_status(&sig).await.unwrap() {
                        err.unwrap();
                        true
                    } else {
                        false
                    }
                } else {
                    lite_client.confirm_transaction(sig.to_string()).await
                };

                if confirmed {
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
        metrics.txs_sent = NUM_OF_TXS as u64;

        metrics
    });

    let (send_fut, confirm_fut) = tokio::join!(send_fut, confirm_fut);
    let time_to_send_txs = send_fut.unwrap();
    let mut metrics = confirm_fut.unwrap();
    metrics.time_to_send_txs = time_to_send_txs.as_secs_f64();
    metrics.calc_tps();

    metrics
}
