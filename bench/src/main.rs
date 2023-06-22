use bench::{
    cli::Args,
    helpers::BenchHelper,
    metrics::{AvgMetric, Metric, TxMetricData},
};
use clap::Parser;
use dashmap::DashMap;
use futures::future::join_all;
use log::{error, info};
use solana_client::{nonblocking::tpu_client::TpuClient, connection_cache::{self, ConnectionCache}, tpu_client::TpuClientConfig};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig, hash::Hash, signature::Keypair, signer::Signer,
    slot_history::Slot, transaction::Transaction, message::Message, compute_budget, instruction::{Instruction, AccountMeta}, pubkey::Pubkey,
};
use std::{sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
}, str::FromStr, net::{IpAddr, Ipv4Addr}};
use tokio::{
    sync::{mpsc::UnboundedSender, RwLock},
    time::{Duration, Instant},
};
use solana_quic_client::{QuicConfig, QuicConnectionManager, QuicPool};

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
    } = Args::parse();

    let mut run_interval_ms = tokio::time::interval(Duration::from_millis(run_interval_ms));

    info!("Connecting to {lite_rpc_addr}");

    let mut avg_metric = AvgMetric::default();

    let mut tasks = vec![];

    let funded_payer = BenchHelper::get_payer().await.unwrap();
    println!("payer : {}", funded_payer.pubkey());

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
        tasks.push(tokio::spawn(bench(
            rpc_client.clone(),
            tx_count,
            funded_payer,
            seed as u64,
            block_hash.clone(),
            current_slot.clone(),
            tx_log_sx.clone(),
            log_transactions,
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

#[derive(Clone, Debug, Copy)]
struct TxSendData {
    sent_duration: Duration,
    sent_instant: Instant,
    sent_slot: Slot,
}

pub type QuicTpuClient = TpuClient<QuicPool, QuicConnectionManager, QuicConfig>;
pub type QuicConnectionCache = ConnectionCache;

#[allow(clippy::too_many_arguments)]
async fn bench(
    rpc_client: Arc<RpcClient>,
    tx_count: usize,
    funded_payer: Keypair,
    seed: u64,
    block_hash: Arc<RwLock<Hash>>,
    current_slot: Arc<AtomicU64>,
    tx_metric_sx: UnboundedSender<TxMetricData>,
    log_txs: bool,
) -> Metric {
    let map_of_txs = Arc::new(DashMap::new());
    let identity = Keypair::from_bytes(&[141,179,120,222,239,132,255,144,140,56,166,225,180,229,14,145,191,161,206,42,157,56,201,93,215,41,118,192,192,20,130,214,130,62,99,54,224,161,27,84,137,174,81,66,73,59,26,72,112,3,10,223,126,173,83,209,219,147,71,192,124,225,182,12]).unwrap();
    let connection_cache = ConnectionCache::new_with_client_options(
        4,
        None,
        Some((&identity, IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))),
        None,
    );
    let quic_connection_cache =
        if let ConnectionCache::Quic(connection_cache) = connection_cache {
            Some(connection_cache)
        } else {
            None
        };

    let tpu_client = Arc::new(
        TpuClient::new_with_connection_cache(
            rpc_client.clone(),
            "ws://34.127.23.120:8900",
            solana_client::tpu_client::TpuClientConfig { fanout_slots: 16 },
            quic_connection_cache.unwrap(),
        )
        .await
        .unwrap(),
    );

    // transaction sender task
    {
        let map_of_txs = map_of_txs.clone();
        let rpc_client = rpc_client.clone();
        let current_slot = current_slot.clone();
        tokio::spawn(async move {
            let map_of_txs = map_of_txs.clone();
            let ix1 = compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(1_000_000);

            let program_id = Pubkey::from_str("3D5RvD4dkri2m7M5n2WcJzPburUqJrjoTHSifxCFXQM5").unwrap();

            let iters: u64 = 350;
            let ix_data = iters.to_le_bytes();
            let attacker = Instruction::new_with_bytes( program_id, ix_data.as_slice(), vec![ 
                AccountMeta::new_readonly(Pubkey::from_str("6C3UV6UuMsJUg8M2LAk8aCznKn6xvgfrvJJ9tHvr6MkG").unwrap(), false),
                AccountMeta::new_readonly(Pubkey::from_str("97Lr8PDnGCXuC3NDMTggmXCtNu2FugJSiRfheH4oNMqD").unwrap(), false),
            ]);

            let tpu_client = tpu_client.clone();

            for i in 0..tx_count {
                let ix2 = compute_budget::ComputeBudgetInstruction::set_compute_unit_price((i + 1) as u64);
                let blockhash = { *block_hash.read().await };
                let message = Message::new( &[ix1.clone(), ix2, attacker.clone()], Some(&funded_payer.pubkey()));
                let tx = Transaction::new(&[&funded_payer], message, blockhash);
                let start_time = Instant::now();
                let signature = tx.signatures[0];
                if tpu_client.send_transaction(&tx).await {
                    map_of_txs.insert(
                        signature,
                        TxSendData {
                            sent_duration: start_time.elapsed(),
                            sent_instant: Instant::now(),
                            sent_slot: current_slot.load(std::sync::atomic::Ordering::Relaxed),
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
                    metric.add_successful_transaction(tx_data.sent_duration, time_to_confirm);

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
        metric.add_unsuccessful_transaction(tx.sent_duration);
    }
    metric.finalize();
    metric
}
