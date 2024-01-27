use std::collections::HashMap;
use std::fs::File;
use std::time::Duration;
use csv::Writer;

use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::slot_history::Slot;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};

use crate::helpers::BenchHelper;

use super::Strategy;
use crate::cli::{LiteRpcArgs, RpcArgs};
use crate::strategies::tc1::{Tc1, Tc1Result};

#[derive(Debug, serde::Serialize)]
pub struct Tc2Result {
    lite_rpc: Vec<RpcStat>,
    rpc: Vec<RpcStat>,
    avg_lite_rpc: RpcStat,
    avg_rpc: RpcStat,
    runs: usize,
    bulk: usize,
    retries: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct RpcStat {
    time: Duration,
    mode_slot: u64,
    confirmed: u64,
    unconfirmed: u64,
    failed: u64,
}

/// send bulk (100-200) * 5 txs; measure the confirmation rate
#[derive(clap::Args, Debug)]
pub struct Tc2 {
    /// amount of txs to send to a node
    #[arg(short = 'b', long, default_value_t = 200)]
    bulk: usize,
    /// number of times to send the bulk
    #[arg(short = 'R', long, default_value_t = 5)]
    runs: usize,
    /// confirmation retries
    #[arg(short = 'c', long, default_value_t = 5)]
    retries: usize,

    #[command(flatten)]
    rpc_args: RpcArgs,

    #[command(flatten)]
    lite_rpc_args: LiteRpcArgs,
}

impl Tc2 {
    pub async fn send_bulk_txs(&self, rpc: &RpcClient, payer: &Keypair) -> anyhow::Result<RpcStat> {
        let hash = rpc.get_latest_blockhash().await?;
        let mut rng = BenchHelper::create_rng(None);
        let txs =
            BenchHelper::generate_txs(self.bulk, payer, hash, &mut rng, self.rpc_args.tx_size);

        let instant = tokio::time::Instant::now();

        let txs = BenchHelper::send_and_confirm_transactions(
            rpc,
            &txs,
            CommitmentConfig::confirmed(),
            Some(self.retries),
        )
        .await?;

        let time = instant.elapsed();

        let (mut confirmed, mut unconfirmed, mut failed) = (0, 0, 0);
        let mut slot_hz: HashMap<Slot, u64> = Default::default();

        for tx in txs {
            match tx {
                Ok(Some(status)) => {
                    if status.satisfies_commitment(CommitmentConfig::confirmed()) {
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
            time,
            mode_slot,
            confirmed,
            unconfirmed,
            failed,
        })
    }

    fn get_stats_avg(stats: &[RpcStat]) -> RpcStat {
        let len = stats.len();

        let mut avg = RpcStat {
            time: Duration::default(),
            mode_slot: 0,
            confirmed: 0,
            unconfirmed: 0,
            failed: 0,
        };

        for stat in stats {
            avg.time += stat.time;
            avg.confirmed += stat.confirmed;
            avg.unconfirmed += stat.unconfirmed;
            avg.failed += stat.failed;
        }

        avg.time /= len as u32;
        avg.confirmed /= len as u64;
        avg.unconfirmed /= len as u64;
        avg.failed /= len as u64;

        avg
    }
}

#[async_trait::async_trait]
impl Strategy for Tc2 {
    type Output = Tc2Result;

    async fn execute(&self) -> anyhow::Result<Self::Output> {
        let lite_rpc = RpcClient::new(self.lite_rpc_args.lite_rpc_addr.clone());
        let rpc = RpcClient::new(self.rpc_args.rpc_addr.clone());

        let payer = BenchHelper::get_payer(&self.rpc_args.payer).await?;

        let mut use_lite_rpc = true;

        let mut rpc_results = Vec::with_capacity(self.runs);
        let mut lite_rpc_results = Vec::with_capacity(self.runs);

        for _ in 0..self.runs {
            let (rpc, list) = if use_lite_rpc {
                (&lite_rpc, &mut lite_rpc_results)
            } else {
                (&rpc, &mut rpc_results)
            };

            let stat = self.send_bulk_txs(rpc, &payer).await?;
            list.push(stat);

            use_lite_rpc = !use_lite_rpc;
        }

        let avg_lite_rpc = Self::get_stats_avg(&lite_rpc_results);
        let avg_rpc = Self::get_stats_avg(&rpc_results);

        Ok(Tc2Result {
            lite_rpc: lite_rpc_results,
            rpc: rpc_results,
            avg_lite_rpc,
            avg_rpc,
            runs: self.runs,
            bulk: self.bulk,
            retries: self.retries,
        })
    }
}

impl Tc2 {
    pub fn write_csv(csv_writer: &mut Writer<File>, result: &Tc2Result) -> anyhow::Result<()> {
        // TODO
        Ok(())
    }
}
