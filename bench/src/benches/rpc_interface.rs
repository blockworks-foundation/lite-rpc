use crate::benches::tx_status_websocket_collector::start_tx_status_collector;
use anyhow::{bail, Context, Error};

use futures::future::join_all;
use futures::TryFutureExt;
use itertools::Itertools;
use log::{debug, trace, warn};

use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client::rpc_client::SerializableTransaction;
use solana_rpc_client_api::client_error::ErrorKind;
use solana_rpc_client_api::config::RpcSendTransactionConfig;

use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use solana_transaction_status::TransactionConfirmationStatus;
use std::collections::{HashMap, HashSet};

use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use url::Url;

pub fn create_rpc_client(rpc_url: &Url) -> RpcClient {
    RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::confirmed())
}

#[derive(Clone)]
pub enum ConfirmationResponseFromRpc {
    // RPC error on send_transaction
    SendError(Arc<ErrorKind>),
    // (sent slot at confirmed commitment, confirmed slot, ..., ...)
    // transaction_confirmation_status is "confirmed"
    Success(Slot, Slot, TransactionConfirmationStatus, Duration),
    // timout waiting for confirmation status
    Timeout(Duration),
}

pub async fn send_and_confirm_bulk_transactions(
    rpc_client: &RpcClient,
    tx_status_websocket_addr: Url,
    payer_pubkey: Pubkey,
    txs: &[VersionedTransaction],
    max_timeout: Duration,
) -> anyhow::Result<Vec<(Signature, ConfirmationResponseFromRpc)>> {
    debug!(
        "send_transaction for {} txs with timeout {:.03}s",
        txs.len(),
        max_timeout.as_secs_f32()
    );

    let send_config = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: None,
        encoding: None,
        max_retries: None,
        min_context_slot: None,
    };

    let tx_listener_startup_token = CancellationToken::new();

    // note: we get confirmed but not finalized
    let tx_listener_startup_token_cp = tx_listener_startup_token.clone();
    let (tx_status_map, _jh_collector) = start_tx_status_collector(
        tx_status_websocket_addr.clone(),
        payer_pubkey,
        CommitmentConfig::confirmed(),
        tx_listener_startup_token_cp,
    )
    .await;

    // waiting for thread to cancel the token
    tx_listener_startup_token.cancelled().await;

    trace!("Waiting for next slot before sending transactions ..");
    let send_slot = poll_next_slot_start(rpc_client)
        .await
        .context("poll for next start slot")?;
    trace!("Send slot: {}", send_slot);

    let started_at = Instant::now();
    trace!(
        "Sending {} transactions via RPC (retries=off) ..",
        txs.len()
    );
    let batch_sigs_or_fails = join_all(txs.iter().map(|tx| {
        rpc_client
            .send_transaction_with_config(tx, send_config)
            .map_err(|e| e.kind)
    }))
    .await;

    let after_send_slot = rpc_client
        .get_slot_with_commitment(CommitmentConfig::confirmed())
        .await
        .context("get slot afterwards")?;

    if after_send_slot - send_slot > 0 {
        warn!(
            "Slot advanced during sending transactions: {} -> {}",
            send_slot, after_send_slot
        );
    } else {
        debug!(
            "Slot did not advance during sending transactions: {} -> {}",
            send_slot, after_send_slot
        );
    }

    let num_sent_ok = batch_sigs_or_fails
        .iter()
        .filter(|sig_or_fail| sig_or_fail.is_ok())
        .count();
    let num_sent_failed = batch_sigs_or_fails
        .iter()
        .filter(|sig_or_fail| sig_or_fail.is_err())
        .count();

    for (i, tx_sig) in txs.iter().enumerate() {
        let tx_sent = batch_sigs_or_fails[i].is_ok();
        if tx_sent {
            debug!("- tx_sent {}", tx_sig.get_signature());
        } else {
            debug!("- tx_fail {}", tx_sig.get_signature());
        }
    }
    let elapsed = started_at.elapsed();
    debug!(
        "send_transaction successful for {} txs in {:.03}s",
        num_sent_ok,
        elapsed.as_secs_f32()
    );
    debug!(
        "send_transaction failed to send {} txs in {:.03}s",
        num_sent_failed,
        elapsed.as_secs_f32()
    );

    if num_sent_failed > 0 {
        warn!(
            "Some transactions failed to send: {} out of {}",
            num_sent_failed,
            txs.len()
        );
        bail!("Failed to send all transactions");
    }

    let mut pending_status_set: HashSet<Signature> = HashSet::new();
    batch_sigs_or_fails
        .iter()
        .filter(|sig_or_fail| sig_or_fail.is_ok())
        .for_each(|sig_or_fail| {
            pending_status_set.insert(sig_or_fail.as_ref().unwrap().to_owned());
        });
    let mut result_status_map: HashMap<Signature, ConfirmationResponseFromRpc> = HashMap::new();

    // items get moved from pending_status_set to result_status_map

    let started_at = Instant::now();
    let timeout_at = started_at + max_timeout;
    // "poll" the status dashmap which gets updated by the tx status collector task
    'polling_loop: for iteration in 1.. {
        let iteration_ends_at = started_at + Duration::from_millis(iteration * 100);
        assert_eq!(
            pending_status_set.len() + result_status_map.len(),
            num_sent_ok,
            "Items must move between pending+result"
        );
        let elapsed = started_at.elapsed();

        for multi in tx_status_map.iter() {
            // note that we will see tx_sigs we did not send
            let (tx_sig, confirmed_slot) = multi.pair();

            // status is confirmed
            if pending_status_set.remove(tx_sig) {
                trace!(
                    "websocket source tx status for sig {:?} and confirmed_slot: {:?}",
                    tx_sig,
                    confirmed_slot
                );
                let prev_value = result_status_map.insert(
                    *tx_sig,
                    ConfirmationResponseFromRpc::Success(
                        send_slot,
                        *confirmed_slot,
                        // note: this is not optimal as we do not cover finalized here
                        TransactionConfirmationStatus::Confirmed,
                        elapsed,
                    ),
                );
                assert!(prev_value.is_none(), "Must not override existing value");
            }
        } // -- END for tx_status_map loop

        if pending_status_set.is_empty() {
            debug!(
                "All transactions confirmed after {:?}",
                started_at.elapsed()
            );
            break 'polling_loop;
        }

        if Instant::now() > timeout_at {
            warn!(
                "Timeout waiting for transactions to confirm after {:?}",
                started_at.elapsed()
            );
            break 'polling_loop;
        }

        tokio::time::sleep_until(iteration_ends_at).await;
    } // -- END polling loop

    let total_time_elapsed_polling = started_at.elapsed();

    // all transactions which remain in pending list are considered timed out
    for tx_sig in pending_status_set.clone() {
        pending_status_set.remove(&tx_sig);
        result_status_map.insert(
            tx_sig,
            ConfirmationResponseFromRpc::Timeout(total_time_elapsed_polling),
        );
    }

    let result_as_vec = batch_sigs_or_fails
        .into_iter()
        .enumerate()
        .map(|(i, sig_or_fail)| match sig_or_fail {
            Ok(tx_sig) => {
                let confirmation = result_status_map
                    .get(&tx_sig)
                    .expect("consistent map with all tx")
                    .clone()
                    .to_owned();
                (tx_sig, confirmation)
            }
            Err(send_error) => {
                let tx_sig = txs[i].get_signature();
                let confirmation = ConfirmationResponseFromRpc::SendError(Arc::new(send_error));
                (*tx_sig, confirmation)
            }
        })
        .collect_vec();

    Ok(result_as_vec)
}

pub async fn poll_next_slot_start(rpc_client: &RpcClient) -> Result<Slot, Error> {
    let started_at = Instant::now();
    let mut last_slot: Option<Slot> = None;
    let mut i = 1;
    // try to catch slot start
    let send_slot = loop {
        if i > 500 {
            bail!("Timeout waiting for slot change");
        }

        let iteration_ends_at = started_at + Duration::from_millis(i * 30);
        let slot = rpc_client
            .get_slot_with_commitment(CommitmentConfig::confirmed())
            .await?;
        trace!(".. polling slot {}", slot);
        if let Some(last_slot) = last_slot {
            if last_slot + 1 == slot {
                break slot;
            }
        }
        last_slot = Some(slot);
        tokio::time::sleep_until(iteration_ends_at).await;
        i += 1;
    };
    Ok(send_slot)
}
