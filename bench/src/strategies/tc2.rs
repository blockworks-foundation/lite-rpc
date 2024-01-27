use std::collections::HashMap;

use indicatif::MultiProgress;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use solana_sdk::slot_history::Slot;
use solana_transaction_status::TransactionConfirmationStatus;

use crate::helpers::BenchHelper;

use super::Strategy;
use crate::cli::RpcArgs;

#[derive(Debug, serde::Serialize, Clone)]
pub struct RpcStat {
    rpc: String,
    time_ns: u128,
    mode_slot: u64,
    confirmed: u64,
    unconfirmed: u64,
    failed: u64,
}

pub type Tc2Result = RpcStat;

/// send bulk (100-200) * 5 txs; measure the confirmation rate
#[derive(clap::Args, Debug)]
pub struct Tc2 {
    /// amount of txs to send to a node
    #[arg(short = 'b', long, default_value_t = 200)]
    bulk: usize,
    /// number of times to send the bulk
    #[arg(short = 'R', long, default_value_t = 5)]
    runs: usize,

    #[command(flatten)]
    rpc_args: RpcArgs,
}

impl Tc2 {
    pub async fn send_bulk_txs(
        &self,
        rpc: &RpcClient,
        payer: &Keypair,
        multi_progress_bar: &MultiProgress,
    ) -> anyhow::Result<RpcStat> {
        let hash = rpc.get_latest_blockhash().await?;
        let mut rng = BenchHelper::create_rng(None);
        let txs =
            BenchHelper::generate_txs(self.bulk, payer, hash, &mut rng, self.rpc_args.tx_size);

        let instant = tokio::time::Instant::now();

        let txs = BenchHelper::send_and_confirm_transactions(
            rpc,
            &txs,
            TransactionConfirmationStatus::Confirmed,
            self.rpc_args.confirmation_retries,
            multi_progress_bar,
        )
        .await?;

        let time = instant.elapsed();

        let (mut confirmed, mut unconfirmed, mut failed) = (0, 0, 0);
        let mut slot_hz: HashMap<Slot, u64> = Default::default();

        for tx in txs {
            match tx {
                Ok(Some(status)) => {
                    if status.confirmation_status() == TransactionConfirmationStatus::Confirmed {
                        confirmed += 1;
                        *slot_hz.entry(status.slot).or_default() += 1;
                    } else {
                        unconfirmed += 1;
                    }
                }
                Ok(None) => {
                    unconfirmed += 1;
                }
                Err(_) => {
                    failed += 1;
                }
            }
        }

        let mode_slot = slot_hz
            .into_iter()
            .max_by_key(|(_, v)| *v)
            .map(|(k, _)| k)
            .unwrap_or_default();

        Ok(RpcStat {
            rpc: rpc.url().to_string(),
            time_ns: time.as_nanos(),
            mode_slot,
            confirmed,
            unconfirmed,
            failed,
        })
    }

    fn get_stats_avg(stats: &[RpcStat]) -> RpcStat {
        let len = stats.len();

        let mut avg = stats[0].clone();

        for stat in stats.iter().skip(1) {
            avg.time_ns += stat.time_ns;
            avg.confirmed += stat.confirmed;
            avg.unconfirmed += stat.unconfirmed;
            avg.failed += stat.failed;
        }

        avg.rpc = "avg".to_string();
        avg.time_ns /= len as u128;
        avg.confirmed /= len as u64;
        avg.unconfirmed /= len as u64;
        avg.failed /= len as u64;

        avg
    }

    fn get_results(mut stats: Vec<RpcStat>) -> Vec<Tc2Result> {
        let avg = Self::get_stats_avg(&stats);

        stats.push(avg);

        stats
    }
}

#[async_trait::async_trait]
impl Strategy for Tc2 {
    async fn execute(&self) -> anyhow::Result<Vec<serde_json::Value>> {
        let rpc = RpcClient::new(self.rpc_args.rpc_addr.clone());

        let payer = BenchHelper::get_payer(&self.rpc_args.payer).await?;

        let multi_progress_bar = MultiProgress::new();

        let mut rpc_results = Vec::with_capacity(self.runs);

        for _ in 0..self.runs {
            let stat = self
                .send_bulk_txs(&rpc, &payer, &multi_progress_bar)
                .await?;

            rpc_results.push(stat);
        }

        let results = Self::get_results(rpc_results)
            .into_iter()
            .map(|r| serde_json::to_value(r).unwrap())
            .collect::<Vec<_>>();

        Ok(results)
    }
}
