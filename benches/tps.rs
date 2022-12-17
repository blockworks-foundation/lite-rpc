use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use bench_utils::{
    helpers::{generate_txs, new_funded_payer},
    metrics::{AvgMetric, Metric},
};
use log::info;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_client::SerializableTransaction};
use solana_sdk::native_token::LAMPORTS_PER_SOL;

use lite_client::{LiteClient, LOCAL_LIGHT_RPC_ADDR};
use simplelog::*;
use tokio::sync::mpsc;

const NUM_OF_TXS: usize = 20_000;
const NUM_OF_RUNS: usize = 5;
const CSV_FILE_NAME: &str = "metrics.csv";

#[tokio::main]
async fn main() {
    TermLogger::init(
        LevelFilter::Info,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
    .unwrap();

    let lite_client = Arc::new(LiteClient(RpcClient::new(LOCAL_LIGHT_RPC_ADDR.to_string())));
    let mut csv_writer = csv::Writer::from_path(CSV_FILE_NAME).unwrap();

    let mut avg_metric = AvgMetric::default();

    for run_num in 0..NUM_OF_RUNS {
        let metric = foo(lite_client.clone()).await;
        info!("Run {run_num}: Sent and Confirmed {NUM_OF_TXS} tx(s) in {metric:?}",);
        avg_metric += &metric;
        csv_writer.serialize(metric).unwrap();
    }

    let avg_metric = Metric::from(avg_metric);

    info!("Avg Metric {avg_metric:?}",);
    csv_writer.serialize(avg_metric).unwrap();

    csv_writer.flush().unwrap();
}

async fn foo(lite_client: Arc<LiteClient>) -> Metric {
    let funded_payer = new_funded_payer(&lite_client, LAMPORTS_PER_SOL * 2000)
        .await
        .unwrap();

    let txs = generate_txs(NUM_OF_TXS, &lite_client.0, &funded_payer)
        .await
        .unwrap();

    let mut un_confirmed_txs: HashMap<String, Option<Instant>> = HashMap::with_capacity(txs.len());

    for tx in &txs {
        un_confirmed_txs.insert(tx.get_signature().to_string(), None);
    }

    let start_time = Instant::now();

    info!("Sending and Confirming {NUM_OF_TXS} tx(s)");

    let send_fut = {
        let lite_client = lite_client.clone();
        tokio::spawn(async move {
            for tx in txs {
                lite_client.send_transaction(&tx).await.unwrap();
                info!("Tx {}", &tx.signatures[0]);
            }
            info!("Sent {NUM_OF_TXS} tx(s)");
        })
    };

    let (metrics_send, mut metrics_recv) = mpsc::channel(1);

    let confirm_fut = tokio::spawn(async move {
        let mut metrics = Metric::default();

        while !un_confirmed_txs.is_empty() {
            let mut to_remove_txs = Vec::new();

            for (sig, time_elapsed_since_last_confirmed) in un_confirmed_txs.iter_mut() {
                if time_elapsed_since_last_confirmed.is_none() {
                    *time_elapsed_since_last_confirmed = Some(Instant::now())
                }

                if lite_client.confirm_transaction(sig.clone()).await {
                    metrics.txs_confirmed += 1;
                    to_remove_txs.push(sig.clone());
                } else if time_elapsed_since_last_confirmed.unwrap().elapsed()
                    > Duration::from_secs(3)
                {
                    metrics.txs_un_confirmed += 1;
                    to_remove_txs.push(sig.clone());
                }
            }

            for to_remove_tx in to_remove_txs {
                un_confirmed_txs.remove(&to_remove_tx);
            }
        }

        metrics.time_elapsed_sec = start_time.elapsed().as_secs_f64();
        metrics.txs_sent = NUM_OF_TXS as u64;
        metrics.calc_tps();

        metrics_send.send(metrics).await.unwrap();
    });

    let (res1, res2) = tokio::join!(send_fut, confirm_fut);
    res1.unwrap();
    res2.unwrap();

    metrics_recv.recv().await.unwrap()
}
