use crate::{create_rng, generate_txs, BenchmarkTransactionParams};
use log::{debug, info, trace, warn};
use std::ops::Add;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Context};

use crate::benches::rpc_interface::{
    send_and_confirm_bulk_transactions, ConfirmationResponseFromRpc,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::{read_keypair_file, Keypair, Signature, Signer};

#[derive(Debug, serde::Serialize)]
pub struct RpcStat {
//     confirmation_time: f32,
//     mode_slot: u64,
//     confirmed: u64,
//     unconfirmed: u64,
//     failed: u64,
    tx_sent: u64,
    tx_confirmed: u64,
    // in ms
    average_confirmation_time: f32,
    // in slots
    average_slot_confirmation_time: f32,
    tx_send_errors: u64,
    tx_unconfirmed: u64,
}

/// TC2 send multiple runs of num_txns, measure the confirmation rate
pub async fn confirmation_rate(
    payer_path: &Path,
    rpc_url: String,
    tx_params: BenchmarkTransactionParams,
    txs_per_round: usize,
    num_of_runs: usize,
) -> anyhow::Result<()> {
    warn!("THIS IS WORK IN PROGRESS");

    assert!(num_of_runs > 0, "num_of_runs must be greater than 0");

    let rpc = Arc::new(RpcClient::new(rpc_url));
    info!("RPC: {}", rpc.as_ref().url());

    let payer: Arc<Keypair> = Arc::new(read_keypair_file(payer_path).unwrap());
    info!("Payer: {}", payer.pubkey().to_string());

    let mut rpc_results = Vec::with_capacity(num_of_runs);

    for _ in 0..num_of_runs {
        match send_bulk_txs_and_wait(&rpc, &payer, txs_per_round, &tx_params).await.context("send bulk tx and wait") {
            Ok(stat) => {
                rpc_results.push(stat);
            }
            Err(err) => {
                warn!("Failed to send bulk txs and wait - no rpc stats available: {}", err);
            }
        }
    }

    if rpc_results.len() > 0 {
        info!("avg_rpc: {:?}", calc_stats_avg(&rpc_results));
    } else {
        info!("avg_rpc: n/a");
    }
    Ok(())
}

pub async fn send_bulk_txs_and_wait(
    rpc: &RpcClient,
    payer: &Keypair,
    num_txns: usize,
    tx_params: &BenchmarkTransactionParams,
) -> anyhow::Result<RpcStat> {
    trace!("Get latest blockhash and generate transactions");
    let hash = rpc.get_latest_blockhash().await
        .map_err(|err| {
            log::error!("Error get latest blockhash : {err:?}");
            err
        })?;
    let mut rng = create_rng(None);
    let txs = generate_txs(num_txns, payer, hash, &mut rng, tx_params);

    trace!("Sending {} transactions in bulk ..", txs.len());
    let tx_and_confirmations_from_rpc: Vec<(Signature, ConfirmationResponseFromRpc)> =
        send_and_confirm_bulk_transactions(rpc, &txs).await.context("send and confirm bulk tx")?;
    trace!("Done sending {} transaction.", txs.len());

    let mut tx_sent = 0;
    let mut tx_send_errors = 0;
    let mut tx_confirmed = 0;
    let mut tx_unconfirmed = 0;
    let mut sum_confirmation_time = Duration::default();
    let mut sum_slot_confirmation_time = 0;
    for (tx_sig, confirmation_response) in tx_and_confirmations_from_rpc {
        match confirmation_response {
            ConfirmationResponseFromRpc::Success(slot_sent, slot_confirmed, commitment_status, confirmation_time) => {
                debug!(
                    "Signature {} confirmed with level {:?} after {:.02}ms, {} slots",
                    tx_sig,
                    commitment_status,
                    confirmation_time.as_secs_f64() * 1000.0,
                    slot_confirmed - slot_sent
                );
                tx_sent += 1;
                tx_confirmed += 1;
                sum_confirmation_time = sum_confirmation_time.add(confirmation_time);
                sum_slot_confirmation_time += slot_confirmed - slot_sent;
            }
            ConfirmationResponseFromRpc::SendError(error_kind) => {
                debug!(
                    "Signature {} failed to get send via RPC: {:?}",
                    tx_sig, error_kind
                );
                tx_send_errors += 1;
            }
            ConfirmationResponseFromRpc::Timeout(elapsed) => {
                debug!(
                    "Signature {} not confirmed after {:.02}ms",
                    tx_sig,
                    elapsed.as_secs_f32() * 1000.0
                );
                tx_sent += 1;
                tx_unconfirmed += 1;
            }
        }
    }

    let average_confirmation_time_ms = if tx_confirmed > 0 {
        sum_confirmation_time.as_secs_f32() * 1000.0 / tx_confirmed as f32
    } else {
        0.0
    };
    let average_slot_confirmation_time = if tx_confirmed > 0 {
        sum_slot_confirmation_time as f32 / tx_confirmed as f32
    } else {
        0.0
    };

    Ok(RpcStat {
        tx_sent,
        tx_send_errors,
        tx_confirmed,
        tx_unconfirmed,
        average_confirmation_time: average_confirmation_time_ms,
        average_slot_confirmation_time,
    })
}

fn calc_stats_avg(stats: &[RpcStat]) -> RpcStat {
    let len = stats.len();

    let mut avg = RpcStat {
        tx_sent: 0,
        tx_send_errors: 0,
        tx_confirmed: 0,
        tx_unconfirmed: 0,
        average_confirmation_time: 0.0,
        average_slot_confirmation_time: 0.0,
    };

    for stat in stats {
        avg.tx_sent += stat.tx_sent;
        avg.tx_send_errors += stat.tx_send_errors;
        avg.tx_confirmed += stat.tx_confirmed;
        avg.tx_unconfirmed += stat.tx_unconfirmed;
        avg.average_confirmation_time += stat.average_confirmation_time;
        avg.average_slot_confirmation_time += stat.average_slot_confirmation_time;
    }

    avg.tx_sent /= len as u64;
    avg.tx_send_errors /= len as u64;
    avg.tx_confirmed /= len as u64;
    avg.tx_unconfirmed /= len as u64;
    avg.average_confirmation_time /= len as f32;
    avg.average_slot_confirmation_time /= len as f32;

    avg
}
