use std::collections::HashMap;
use std::fs::File;
use std::time::Duration;
use csv::Writer;
use log::info;

use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::slot_history::Slot;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};
use solana_sdk::signature::Signer;

use crate::helpers::BenchHelper;

use super::Strategy;
use crate::cli::{CreateTxArgs, LiteRpcArgs, RpcArgs};

#[derive(Debug, serde::Serialize)]
pub struct Tc2Result {
    rpc: Vec<RpcStat>,
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
    create_tx_args: CreateTxArgs,

    #[command(flatten)]
    rpc_args: RpcArgs,
}

impl Tc2 {
    pub async fn send_bulk_txs(&self, rpc: &RpcClient, payer: &Keypair) -> anyhow::Result<RpcStat> {
        let hash = rpc.get_latest_blockhash().await?;
        let mut rng = BenchHelper::create_rng(None);
        let txs =
            BenchHelper::generate_txs(self.bulk, payer, hash, &mut rng, self.create_tx_args.tx_size);

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
        let rpc_client = RpcClient::new(self.rpc_args.rpc_addr.clone());
        info!("RPC: {}", self.rpc_args.rpc_addr);

        let payer = BenchHelper::get_payer(&self.create_tx_args.payer).await?;
        info!("Payer: {}", payer.pubkey().to_string());

        let mut rpc_results = Vec::with_capacity(self.runs);

        for _ in 0..self.runs {
            let stat = self.send_bulk_txs(&rpc_client, &payer).await?;
            rpc_results.push(stat);

        }

        let avg_rpc = Self::get_stats_avg(&rpc_results);

        Ok(Tc2Result {
            rpc: rpc_results,
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

// TODO
#[test]
pub fn serialize_duration() {
    let json_string = serde_json::to_string(&RpcStat {
        time: Duration::from_secs(1),
        mode_slot: 1,
        confirmed: 1,
        unconfirmed: 1,
        failed: 1,
    }).unwrap();

    assert_eq!(json_string, "{\"time\":\"1s\",\"mode_slot\":1,\"confirmed\":1,\"unconfirmed\":1,\"failed\":1}");

}

