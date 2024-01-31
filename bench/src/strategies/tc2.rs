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
use log::{debug, info, trace, warn};

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
    confirmation_time: Duration,
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

        let started_at = tokio::time::Instant::now();

        let tx_and_confirmations_from_rpc: Vec<(Signature, ConfirmationResponseFromRpc)> =
            send_and_confirm_bulk_transactions(
                rpc,
                &txs,
            ).await?;

        let elapsed_total = started_at.elapsed();


        for (tx_sig, confirmation) in &tx_and_confirmations_from_rpc {
            match confirmation {
                ConfirmationResponseFromRpc::Success(_slot, level, elapsed) => {
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

        let (mut confirmed, mut unconfirmed, mut failed) = (0, 0, 0);
        let mut slot_hz: HashMap<Slot, u64> = Default::default();

        for (tx_sig, result_from_rpc) in tx_and_confirmations_from_rpc {
            match result_from_rpc {
                ConfirmationResponseFromRpc::Success(slot, _, _) => {
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
            confirmation_time: elapsed_total,
            mode_slot,
            confirmed,
            unconfirmed,
            failed,
        })
    }

    fn calc_stats_avg(stats: &[RpcStat]) -> RpcStat {
        let len = stats.len();

        let mut avg = RpcStat {
            confirmation_time: Duration::default(),
            mode_slot: 0,
            confirmed: 0,
            unconfirmed: 0,
            failed: 0,
        };

        for stat in stats {
            avg.confirmation_time += stat.confirmation_time;
            avg.confirmed += stat.confirmed;
            avg.unconfirmed += stat.unconfirmed;
            avg.failed += stat.failed;
        }

        avg.confirmation_time /= len as u32;
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
    Success(Slot, TransactionConfirmationStatus, Duration),
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

    let num_sent_ok = batch_sigs_or_fails.iter().filter(|sig_or_fail| sig_or_fail.is_ok()).count();
    let num_sent_failed = batch_sigs_or_fails.iter().filter(|sig_or_fail| sig_or_fail.is_err()).count();

    for (i, tx_sig) in txs.iter().enumerate() {
        let tx_sent = batch_sigs_or_fails[i].is_ok();
        if tx_sent {
            debug!("- tx_sent {}", tx_sig.get_signature());
        } else {
            debug!("- tx_fail {}", tx_sig.get_signature());
        }
    }
    debug!("{} transactions sent successfully in {:.02}ms", num_sent_ok, started_at.elapsed().as_secs_f32() * 1000.0);
    debug!("{} transactions failed to send in {:.02}ms", num_sent_failed, started_at.elapsed().as_secs_f32() * 1000.0);

    if num_sent_failed > 0 {
        warn!("Some transactions failed to send: {} out of {}", num_sent_failed, txs.len());
        bail!("Failed to send all transactions");
    }

    let mut pending_status_set: HashSet<Signature> = HashSet::new();
    batch_sigs_or_fails.iter().filter(|sig_or_fail| sig_or_fail.is_ok()).for_each(|sig_or_fail| {
        pending_status_set.insert(sig_or_fail.as_ref().unwrap().to_owned());
    });
    let mut result_status_map: HashMap<Signature, ConfirmationResponseFromRpc> = HashMap::new();

    // items get moved from pending_status_set to result_status_map

    let started_at = Instant::now();
    let mut iteration = 1;
    'pooling_loop: loop {
        let iteration_ends_at = started_at + Duration::from_millis(iteration * 200);
        assert_eq!(pending_status_set.len() + result_status_map.len(), num_sent_ok, "Items must move between pending+result");
        let tx_batch = pending_status_set.iter().cloned().collect_vec();
        debug!("Request status for batch of remaining {} transactions in iteration {}", tx_batch.len(), iteration);
        let batch_responses = rpc_client.get_signature_statuses(tx_batch.as_slice()).await?;
        let elapsed = started_at.elapsed();

        for (tx_sig, status_response) in zip(tx_batch, batch_responses.value) {
            match status_response {
                Some(tx_status) => {
                    trace!("Some signature status {:?} received for {} after {:.02}ms",
                    tx_status.confirmation_status, tx_sig,
                    elapsed.as_secs_f32() * 1000.0);
                    if !tx_status.satisfies_commitment(CommitmentConfig::confirmed()) {
                        continue 'pooling_loop;
                    }
                    // status is confirmed or finalized
                    pending_status_set.remove(&tx_sig);
                    let prev_value = result_status_map.insert(
                        tx_sig, ConfirmationResponseFromRpc::Success(tx_status.slot, tx_status.confirmation_status(), elapsed));
                    assert!(prev_value.is_none(), "Must not override existing value");
            }
                None => {
                    // None: not yet processed by the cluster
                    trace!("No signature status was received for {} after {:.02}ms - continue waiting",
                    tx_sig, elapsed.as_secs_f32() * 1000.0);
                }
            }
        }

        if pending_status_set.is_empty() {
            debug!("All transactions confirmed after {} iterations", iteration);
            break 'pooling_loop;
        }

        if iteration == 100 {
            debug!("Timeout waiting for transactions to confirmed after {} iterations - giving up on {}", iteration, pending_status_set.len());
            break 'pooling_loop;
        }
        iteration += 1;

        // avg 2 samples per slot
        tokio::time::sleep_until(iteration_ends_at).await;
    } // -- END polling loop

    let total_time_elapsed_polling = started_at.elapsed();

    // all transactions which remain in pending list are considered timed out
    for tx_sig in pending_status_set.clone() {
        pending_status_set.remove(&tx_sig);
        result_status_map.insert(
            tx_sig, ConfirmationResponseFromRpc::Timeout (total_time_elapsed_polling));
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

