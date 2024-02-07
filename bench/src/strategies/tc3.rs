use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::future::join_all;
use indicatif::MultiProgress;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{hash::Hash, signature::Keypair};
use tokio::sync::RwLock;

use crate::{cli::RpcArgs, helpers::BenchHelper};

use super::Strategy;

#[derive(Debug, serde::Serialize)]
pub struct Tc3Result {
    calls_per_second: f64,
    failed: usize,
    success: usize,
}

/// measure how much load the API endpoint can take (send_transaction calls per second)
#[derive(clap::Args, Debug)]
pub struct Tc3 {
    #[command(flatten)]
    rpc_cli_options: RpcArgs,
    #[arg(short = 'T', long, default_value_t = 3000)]
    time_ms: u128,
    /// parallelism
    #[arg(short = 'P', long, default_value_t = 1)]
    parallelism: usize,
}

impl Tc3 {
    pub async fn blockhash_syncer(
        rpc: Arc<RpcClient>,
        current_hash: Arc<RwLock<Hash>>,
        multi_progress_bar: MultiProgress,
        time_ms: u128,
    ) -> Self {
        let progress_bar = multi_progress_bar.add(BenchHelper::create_progress_bar(
            (time_ms as u64) / 400,
            Some(current_hash.read().await.to_string()),
        ));

        loop {
            tokio::time::sleep(Duration::from_millis(400)).await;

            let old_hash = { current_hash.read().await.to_string() };

            let Ok(latest_hash) = rpc.get_latest_blockhash().await else {
                progress_bar.inc(1);
                progress_bar.set_message(format!("failed to get latest blockhash {}", old_hash));
                continue;
            };

            if *old_hash != latest_hash.to_string() {
                progress_bar.set_message(latest_hash.to_string());
                progress_bar.inc(1);

                *current_hash.write().await = latest_hash;
            } else {
                progress_bar.inc(1);
                progress_bar.set_message(format!("same hash: {}", old_hash));
            }
        }
    }

    pub async fn stress(
        rpc: Arc<RpcClient>,
        payer: Arc<Keypair>,
        hash: Arc<RwLock<Hash>>,
        time_ms: u128,
        multi_progress_bar: MultiProgress,
    ) -> Tc3Result {
        let time = tokio::time::Instant::now();

        let mut txs = 0;
        let failed = Arc::new(AtomicUsize::new(0));
        let success = Arc::new(AtomicUsize::new(0));

        let progress_bar = multi_progress_bar.add(BenchHelper::create_progress_bar(
            (time_ms as u64) * 10,
            None,
        ));

        while time.elapsed().as_millis() < time_ms {
            let rpc = rpc.clone();
            let payer = payer.clone();

            let tx = {
                let msg = format!("tx: {txs}");
                let hash = hash.read().await;
                BenchHelper::create_memo_tx_small(msg.as_bytes(), &payer, *hash)
            };

            match rpc.send_transaction(&tx).await {
                Ok(sig) => {
                    progress_bar.set_message(format!("tx: {}", sig));

                    success.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }

            progress_bar.inc(1);

            txs += 1;
        }

        let calls_per_second = (txs * 1000) as f64 / (time_ms as f64);

        Tc3Result {
            calls_per_second,
            failed: failed.load(Ordering::Relaxed),
            success: success.load(Ordering::Relaxed),
        }
    }
}

#[async_trait::async_trait]
impl Strategy for Tc3 {
    async fn execute(&self) -> anyhow::Result<Vec<serde_json::Value>> {
        let rpc = Arc::new(RpcClient::new(self.rpc_cli_options.rpc_addr.clone()));
        let payer = Arc::new(BenchHelper::get_payer(&self.rpc_cli_options.payer).await?);

        let hash = rpc.get_latest_blockhash().await?;
        let hash = Arc::new(RwLock::new(hash));

        let multi_progress_bar = MultiProgress::new();

        let hash_jh = tokio::spawn(Self::blockhash_syncer(
            rpc.clone(),
            hash.clone(),
            multi_progress_bar.clone(),
            self.time_ms,
        ));

        let stress_jhs = (0..self.parallelism)
            .map(|_| {
                let rpc = rpc.clone();
                let payer = payer.clone();
                let hash = hash.clone();
                let multi_progress_bar = multi_progress_bar.clone();

                tokio::spawn(Self::stress(
                    rpc,
                    payer,
                    hash,
                    self.time_ms,
                    multi_progress_bar,
                ))
            })
            .collect::<Vec<_>>();

        let stress_jhs = join_all(stress_jhs);

        tokio::select! {
            _ = hash_jh => {
                anyhow::bail!("blockhash_syncer failed");
            },
            res = stress_jhs => {
                let k = res.into_iter().map(|x| serde_json::to_value(x.unwrap()).unwrap()).collect();

                Ok(k)
            },
        }
    }
}
