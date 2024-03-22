use crate::{create_rng, generate_txs, BenchmarkTransactionParams};
use anyhow::{bail, Error};
use futures::future::join_all;
use futures::TryFutureExt;
use itertools::Itertools;
use log::{debug, info, trace, warn};
use std::collections::{HashMap, HashSet};
use std::iter::zip;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client::rpc_client::SerializableTransaction;
use solana_rpc_client_api::client_error::ErrorKind;
use solana_sdk::signature::{read_keypair_file, Signature, Signer};
use solana_sdk::slot_history::Slot;
use solana_sdk::transaction::Transaction;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};
use solana_transaction_status::TransactionConfirmationStatus;
use tokio::time::Instant;
use crate::benches::rpc_interface::{ConfirmationResponseFromRpc, send_and_confirm_bulk_transactions};

#[derive(Debug, serde::Serialize)]
pub struct RpcStat {
    confirmation_time: f32,
    mode_slot: u64,
    confirmed: u64,
    unconfirmed: u64,
    failed: u64,
}

/// TC2 send multiple runs of num_txns, measure the confirmation rate
pub async fn confirmation_rate(
    payer_path: &Path,
    rpc_url: String,
    tx_params: BenchmarkTransactionParams,
    txns_per_round: usize,
    num_of_runs: usize,
) -> anyhow::Result<()> {
    warn!("THIS IS WORK IN PROGRESS");

    let rpc = Arc::new(RpcClient::new(rpc_url));
    info!("RPC: {}", rpc.as_ref().url());

    let payer: Arc<Keypair> = Arc::new(read_keypair_file(payer_path).unwrap());
    info!("Payer: {}", payer.pubkey().to_string());

    let mut rpc_results = Vec::with_capacity(num_of_runs);

    for _ in 0..num_of_runs {
        let stat: RpcStat =
            send_bulk_txs_and_wait(&rpc, &payer, txns_per_round, &tx_params).await?;
        rpc_results.push(stat);
    }

    info!("avg_rpc: {:?}", calc_stats_avg(&rpc_results));
    Ok(())
}

pub async fn send_bulk_txs_and_wait(
    rpc: &RpcClient,
    payer: &Keypair,
    num_txns: usize,
    tx_params: &BenchmarkTransactionParams,
) -> anyhow::Result<RpcStat> {
    let hash = rpc.get_latest_blockhash().await?;
    let mut rng = create_rng(None);
    let txs = generate_txs(num_txns, payer, hash, &mut rng, tx_params);

    let started_at = tokio::time::Instant::now();

    let tx_and_confirmations_from_rpc: Vec<(Signature, ConfirmationResponseFromRpc)> =
        send_and_confirm_bulk_transactions(rpc, &txs).await?;

    let elapsed_total = started_at.elapsed();

    for (tx_sig, confirmation) in &tx_and_confirmations_from_rpc {
        match confirmation {
            ConfirmationResponseFromRpc::Success(slots_elapsed, level, elapsed) => {
                debug!(
                    "Signature {} confirmed with level {:?} after {:.02}ms, {} slots",
                    tx_sig,
                    level,
                    elapsed.as_secs_f32() * 1000.0,
                    slots_elapsed
                );
            }
            ConfirmationResponseFromRpc::Timeout(elapsed) => {
                debug!(
                    "Signature {} not confirmed after {:.02}ms",
                    tx_sig,
                    elapsed.as_secs_f32() * 1000.0
                );
            }
            ConfirmationResponseFromRpc::SendError(_) => {
                unreachable!()
            }
        }
    }

    let (mut confirmed, mut unconfirmed, mut failed) = (0, 0, 0);
    let mut slot_hz: HashMap<Slot, u64> = Default::default();

    for (_, result_from_rpc) in tx_and_confirmations_from_rpc {
        match result_from_rpc {
            ConfirmationResponseFromRpc::Success(slot, _, _) => {
                confirmed += 1;
                *slot_hz.entry(slot).or_default() += 1;
            }
            ConfirmationResponseFromRpc::Timeout(_) => {
                unconfirmed += 1;
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
        confirmation_time: elapsed_total.as_secs_f32(),
        mode_slot,
        confirmed,
        unconfirmed,
        failed,
    })
}

fn calc_stats_avg(stats: &[RpcStat]) -> RpcStat {
    let len = stats.len();

    let mut avg = RpcStat {
        confirmation_time: 0.0,
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

    avg.confirmation_time /= len as f32;
    avg.confirmed /= len as u64;
    avg.unconfirmed /= len as u64;
    avg.failed /= len as u64;

    avg
}

