use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::iter::zip;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use anyhow::{bail, Error};
use csv::Writer;
use futures::future::join_all;
use futures::TryFutureExt;
use itertools::Itertools;
use log::{debug, info, warn};

use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client::rpc_client::SerializableTransaction;
use solana_rpc_client_api::client_error::ErrorKind;
use solana_sdk::slot_history::Slot;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};
use solana_sdk::signature::{Signature, Signer};
use solana_transaction_status::{TransactionConfirmationStatus, TransactionStatus};
use tokio::time::Instant;

use crate::helpers::BenchHelper;

use super::Strategy;
use crate::cli::{CreateTxArgs, LiteRpcArgs, RpcArgs};

#[derive(Debug, serde::Serialize)]
pub struct Tc2Result {
    rpc: Vec<RpcStat>,
    avg_rpc: RpcStat,
    runs: usize,
    bulk: usize,
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

    #[command(flatten)]
    create_tx_args: CreateTxArgs,

    #[command(flatten)]
    rpc_args: RpcArgs,
}

impl Tc2 {
    pub async fn send_bulk_txs_and_wait(&self, rpc: &RpcClient, payer: &Keypair) -> anyhow::Result<RpcStat> {
        let hash = rpc.get_latest_blockhash().await?;
        let mut rng = BenchHelper::create_rng(None);
        let txs =
            BenchHelper::generate_txs(self.bulk, payer, hash, &mut rng, self.create_tx_args.tx_size);

        let instant = tokio::time::Instant::now();

        let tx_and_confirmations_from_rpc: Vec<(Signature, ConfirmationResponseFromRpc)> =
            send_and_confirm_bulk_transactions(
                rpc,
                &txs,
            ).await?;

        let time = instant.elapsed();

        let (mut confirmed, mut unconfirmed, mut failed) = (0, 0, 0);
        let mut slot_hz: HashMap<Slot, u64> = Default::default();

        for (tx_sig, result_from_rpc) in tx_and_confirmations_from_rpc {
            match result_from_rpc {
                ConfirmationResponseFromRpc::Confirmed(slot, _, _) => {
                    confirmed += 1;
                    *slot_hz.entry(slot).or_default() += 1;
                }
                ConfirmationResponseFromRpc::Timeout(_) => {
                    unconfirmed +=1 ;
                }
                ConfirmationResponseFromRpc::SendError(_) => {
                    failed += 1;
                }
            }
            //
            // match tx {
            //     Ok(Some(status)) => {
            //         if status.satisfies_commitment(CommitmentConfig::confirmed()) {
            //             confirmed += 1;
            //             *slot_hz.entry(status.slot).or_default() += 1;
            //         } else {
            //             unconfirmed += 1;
            //         }
            //     }
            //     Ok(None) => {
            //         unconfirmed += 1;
            //     }
            //     Err(_) => {
            //         failed += 1;
            //     }
            // }
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

    fn calc_stats_avg(stats: &[RpcStat]) -> RpcStat {
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
            let stat: RpcStat = self.send_bulk_txs_and_wait(&rpc_client, &payer).await?;
            rpc_results.push(stat);
        }

        let avg_rpc = Self::calc_stats_avg(&rpc_results);

        Ok(Tc2Result {
            rpc: rpc_results,
            avg_rpc,
            runs: self.runs,
            bulk: self.bulk,
        })
    }
}

impl Tc2 {
    pub fn write_csv(csv_writer: &mut Writer<File>, result: &Tc2Result) -> anyhow::Result<()> {
        // TODO
        Ok(())
    }
}


#[derive(Clone)]
enum ConfirmationResponseFromRpc {
    SendError(Arc<ErrorKind>),
    Confirmed(Slot, TransactionConfirmationStatus, Duration),
    Timeout(Duration),
}


async fn send_and_confirm_bulk_transactions(
    rpc_client: &RpcClient,
    txs: &[impl SerializableTransaction],
) -> anyhow::Result<Vec<(Signature, ConfirmationResponseFromRpc)>> {

    let started_at = Instant::now();

    let batch_sigs_or_fails = join_all(
        txs.iter()
            .map(|tx| rpc_client.send_transaction(tx).map_err(|e| e.kind))
    ).await;

    debug!("sent {} transactions in {:.02}ms", txs.len(), started_at.elapsed().as_secs_f32() * 1000.0);

    let num_sent_ok = batch_sigs_or_fails.iter().filter(|sig_or_fail| sig_or_fail.is_ok()).count();
    let num_sent_failed = batch_sigs_or_fails.iter().filter(|sig_or_fail| sig_or_fail.is_err()).count();

    debug!("{} transactions sent successfully", num_sent_ok);
    debug!("{} transactions failed to send", num_sent_failed);


    if num_sent_failed > 0 {
        warn!("Some transactions failed to send: {} out of {}", num_sent_failed, txs.len());
        bail!("Failed to send all transactions");
    }

    // TODO use iter api
    let mut pending_status_map: HashSet<Signature> = HashSet::new();
    batch_sigs_or_fails.iter().filter(|sig_or_fail| sig_or_fail.is_ok()).for_each(|sig_or_fail| {
        pending_status_map.insert(sig_or_fail.as_ref().unwrap().to_owned());
    });

    let mut result_status_map: HashMap<Signature, ConfirmationResponseFromRpc> = HashMap::new();

    let started_at = Instant::now();
    'pooling_loop: for i in 1..=100 {
        let iteration_ends_at = started_at + Duration::from_millis(i * 200);
        let tx_batch = pending_status_map.iter().cloned().collect_vec();
        debug!("Request status for batch of {} transactions in iteration {}", tx_batch.len(), i);
        let batch_responses = rpc_client.get_signature_statuses(tx_batch.as_slice()).await?;
        let elapsed = started_at.elapsed();

        for (tx_sig, status_response) in zip(tx_batch, batch_responses.value) {
            match status_response {
                Some(tx_status) => {
                    debug!("Signature status {:?} received for {} after {:.02}ms",
                    tx_status.confirmations, tx_sig,
                    elapsed.as_secs_f32() * 1000.0);
                    if !tx_status.satisfies_commitment(CommitmentConfig::confirmed()) {
                        continue 'pooling_loop;
                    }
                    pending_status_map.remove(&tx_sig);
                    let prev_value = result_status_map.insert(
                        tx_sig, ConfirmationResponseFromRpc::Confirmed(tx_status.slot, tx_status.confirmation_status(), elapsed));
                    assert!(prev_value.is_none(), "Must not override existing value");
            }
                None => {
                    // None: not yet processed by the cluster
                    debug!("Signature status not received for {} after {:.02}ms - continue waiting",
                    tx_sig, elapsed.as_secs_f32() * 1000.0);
                }
            }
        }

        if pending_status_map.is_empty() {
            debug!("All transactions confirmed after {} iterations", i);
            break 'pooling_loop;
        }

        // avg 2 samples per slot
        tokio::time::sleep_until(iteration_ends_at).await;
    } // -- END polling loop


    for tx_sig in pending_status_map.clone() {
        pending_status_map.remove(&tx_sig);
        result_status_map.insert(
            tx_sig, ConfirmationResponseFromRpc::Timeout (started_at.elapsed()));
    }
    for (tx_sig, confirmation) in &result_status_map {
        match confirmation {
            ConfirmationResponseFromRpc::Confirmed(_slot, level, elapsed) => {
                debug!("Signature {} confirmed with level {:?} after {:.02}ms", tx_sig, level, elapsed.as_secs_f32() * 1000.0);
            }
            ConfirmationResponseFromRpc::Timeout(elapsed) => {
                debug!("Signature {} not confirmed after {:.02}ms", tx_sig, elapsed.as_secs_f32() * 1000.0);
            }
            ConfirmationResponseFromRpc::SendError(_) => {
                unreachable!()
            }
        }

    }

    let result_as_vec = batch_sigs_or_fails.into_iter().enumerate()
        .map(|(i, sig_or_fail)| {
            match sig_or_fail {
                Ok(tx_sig) => {
                    let confirmation = result_status_map.get(&tx_sig).expect("consistent map with all tx").clone().to_owned();
                    (tx_sig, confirmation)
                }
                Err(send_error) => {
                    let tx_sig = txs[i].get_signature();
                    let confirmation = ConfirmationResponseFromRpc::SendError(Arc::new(send_error));
                    (*tx_sig, confirmation)
                }
            }
        }).collect_vec();

    Ok(result_as_vec)
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

