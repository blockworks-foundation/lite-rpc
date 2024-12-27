use crate::{create_rng, generate_txs, BenchmarkTransactionParams};
use anyhow::Context;
use log::{debug, info, trace, warn};
use std::ops::Add;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::benches::rpc_interface::{
    send_and_confirm_bulk_transactions, ConfirmationResponseFromRpc,
};
use solana_lite_rpc_util::obfuscate_rpcurl;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::{read_keypair_file, Keypair, Signature, Signer};
use url::Url;

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct Metric {
    pub txs_sent: u64,
    pub txs_confirmed: u64,
    // in ms
    pub average_confirmation_time: f32,
    // in slots
    pub average_slot_confirmation_time: f32,
    pub txs_send_errors: u64,
    pub txs_un_confirmed: u64,
}

/// TC2 send multiple runs of num_txs, measure the confirmation rate
pub async fn confirmation_rate(
    payer_path: &Path,
    rpc_url: String,
    tx_status_websocket_addr: Option<String>,
    tx_params: BenchmarkTransactionParams,
    max_timeout: Duration,
    txs_per_run: usize,
    num_of_runs: usize,
) -> anyhow::Result<()> {
    warn!("THIS IS WORK IN PROGRESS");

    assert!(num_of_runs > 0, "num_of_runs must be greater than 0");

    let rpc = Arc::new(RpcClient::new(rpc_url.clone()));
    info!("RPC: {}", obfuscate_rpcurl(&rpc.as_ref().url()));

    let ws_addr = tx_status_websocket_addr
        .unwrap_or_else(|| rpc_url.replace("http:", "ws:").replace("https:", "wss:"));
    info!("WS ADDR: {}", obfuscate_rpcurl(&ws_addr));

    let payer: Arc<Keypair> = Arc::new(read_keypair_file(payer_path).unwrap());
    info!("Payer: {}", payer.pubkey().to_string());

    let mut rpc_results = Vec::with_capacity(num_of_runs);

    for _ in 0..num_of_runs {
        match send_bulk_txs_and_wait(
            &rpc,
            Url::parse(&ws_addr).expect("Invalid Url"),
            &payer,
            txs_per_run,
            &tx_params,
            max_timeout,
        )
        .await
        .context("send bulk tx and wait")
        {
            Ok(stat) => {
                rpc_results.push(stat);
            }
            Err(err) => {
                warn!(
                    "Failed to send bulk txs and wait - no rpc stats available: {}",
                    err
                );
            }
        }
    }

    if !rpc_results.is_empty() {
        info!("avg_rpc: {:?}", calc_stats_avg(&rpc_results));
    } else {
        info!("avg_rpc: n/a");
    }
    Ok(())
}

pub async fn send_bulk_txs_and_wait(
    rpc: &RpcClient,
    tx_status_websocket_addr: Url,
    payer: &Keypair,
    num_txs: usize,
    tx_params: &BenchmarkTransactionParams,
    max_timeout: Duration,
) -> anyhow::Result<Metric> {
    trace!("Get latest blockhash and generate transactions");
    let recent_blockhash = rpc.get_latest_blockhash().await.map_err(|err| {
        log::error!("Error get latest blockhash : {err:?}");
        err
    })?;
    let mut rng = create_rng(None);
    let txs = generate_txs(num_txs, payer, recent_blockhash, &mut rng, tx_params);

    trace!(
        "Sending {} transactions with blockhash {} to RPC sendTransaction in bulk ..",
        txs.len(),
        recent_blockhash
    );
    let tx_and_confirmations_from_rpc: Vec<(Signature, ConfirmationResponseFromRpc)> =
        send_and_confirm_bulk_transactions(
            rpc,
            tx_status_websocket_addr,
            payer.pubkey(),
            &txs,
            max_timeout,
        )
        .await
        .context("send and confirm bulk tx")?;
    trace!("Done sending {} transaction.", txs.len());

    let mut tx_sent = 0;
    let mut tx_send_errors = 0;
    let mut tx_confirmed = 0;
    let mut tx_unconfirmed = 0;
    let mut sum_confirmation_time = Duration::default();
    let mut sum_slot_confirmation_time = 0;
    for (tx_sig, confirmation_response) in tx_and_confirmations_from_rpc {
        match confirmation_response {
            ConfirmationResponseFromRpc::Success(
                slot_sent,
                slot_confirmed,
                commitment_status,
                confirmation_time,
            ) => {
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
                    "Signature {} not confirmed after {:.03}s",
                    tx_sig,
                    elapsed.as_secs_f32()
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

    Ok(Metric {
        txs_sent: tx_sent,
        txs_send_errors: tx_send_errors,
        txs_confirmed: tx_confirmed,
        txs_un_confirmed: tx_unconfirmed,
        average_confirmation_time: average_confirmation_time_ms,
        average_slot_confirmation_time,
    })
}

fn calc_stats_avg(stats: &[Metric]) -> Metric {
    let len = stats.len();

    let mut avg = Metric {
        txs_sent: 0,
        txs_send_errors: 0,
        txs_confirmed: 0,
        txs_un_confirmed: 0,
        average_confirmation_time: 0.0,
        average_slot_confirmation_time: 0.0,
    };

    for stat in stats {
        avg.txs_sent += stat.txs_sent;
        avg.txs_send_errors += stat.txs_send_errors;
        avg.txs_confirmed += stat.txs_confirmed;
        avg.txs_un_confirmed += stat.txs_un_confirmed;
        avg.average_confirmation_time += stat.average_confirmation_time;
        avg.average_slot_confirmation_time += stat.average_slot_confirmation_time;
    }

    avg.txs_sent /= len as u64;
    avg.txs_send_errors /= len as u64;
    avg.txs_confirmed /= len as u64;
    avg.txs_un_confirmed /= len as u64;
    avg.average_confirmation_time /= len as f32;
    avg.average_slot_confirmation_time /= len as f32;

    avg
}
