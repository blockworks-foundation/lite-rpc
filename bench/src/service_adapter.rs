// adapter code for all from benchrunner-service

use crate::helpers::BenchHelper;
use crate::metrics::{AvgMetric, Metric, TxMetricData};
use crate::oldbench;
use crate::oldbench::TransactionSize;
use crate::tx_size::TxSize;
use log::{debug, info};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::Instant;

pub struct BenchConfig {
    pub tenant: String,
    pub tx_count: usize,
    pub cu_price_micro_lamports: u64,
}

pub async fn bench_servicerunner(
    bench_config: &BenchConfig,
    rpc_addr: String,
    funded_payer: Keypair,
    size_tx: TxSize,
) -> Metric {
    let started_at = Instant::now();

    let transaction_size = match size_tx {
        TxSize::Small => TransactionSize::Small,
        TxSize::Large => TransactionSize::Large,
    };

    debug!("Payer: {}", funded_payer.pubkey());

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        rpc_addr.clone(),
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

    {
        // TODO what todo
        // not used unless log_txs is set to true
        let (tx_log_sx_null, _tx_log_rx) = tokio::sync::mpsc::unbounded_channel::<TxMetricData>();

        let metric = oldbench::bench(
            rpc_client.clone(),
            bench_config.tx_count,
            funded_payer,
            started_at.elapsed().as_micros() as u64,
            block_hash.clone(),
            current_slot.clone(),
            tx_log_sx_null,
            false, // log_transactions
            transaction_size,
            bench_config.cu_price_micro_lamports,
        )
        .await;

        metric
    }
}
