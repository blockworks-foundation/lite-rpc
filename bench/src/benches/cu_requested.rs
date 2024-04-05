use anyhow::Context;
use log::{debug, info, trace, warn};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction, ed25519_instruction::{Ed25519SignatureOffsets, DATA_START, PUBKEY_SERIALIZED_SIZE, SIGNATURE_SERIALIZED_SIZE}, hash::Hash, instruction::Instruction, message::v0, pubkey::Pubkey
};
use std::{ops::Add, str::FromStr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::{benches::rpc_interface::{
    send_and_confirm_bulk_transactions, ConfirmationResponseFromRpc,
}, create_memo_tx_large, MEMO_PROGRAM_ID};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    signature::{read_keypair_file, Keypair, Signature, Signer},
    transaction::VersionedTransaction,
};

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

/// Sends multiple txns with varying CU requested and measures the confirmations by CU
pub async fn compute_units_requested(
    payer_path: &Path,
    rpc_url: String,
    num_of_runs: usize,
    cu_samples: usize,
    cu_price: u64,
    max_timeout: Duration,
) -> anyhow::Result<()> {
    warn!("THIS IS WORK IN PROGRESS");

    assert!(num_of_runs > 0, "num_of_runs must be greater than 0");
    assert!(
        cu_samples <= CU_LIMITS.len(),
        "cu_samples must be <= {}, as the max CU for a transaction is 1.4M", CU_LIMITS.len()
    );

    let rpc = Arc::new(RpcClient::new(rpc_url));
    info!("RPC: {}", rpc.as_ref().url());

    let payer: Arc<Keypair> = Arc::new(read_keypair_file(payer_path).unwrap());
    info!("Payer: {}", payer.pubkey().to_string());

    let mut rpc_results = Vec::with_capacity(num_of_runs);

    for _ in 0..num_of_runs {
        match send_bulk_txs_and_wait(&rpc, &payer, cu_samples, cu_price, max_timeout)
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
        // info!("avg_rpc: {:?}", calc_stats_avg(&rpc_results));
    } else {
        info!("avg_rpc: n/a");
    }
    Ok(())
}

pub async fn send_bulk_txs_and_wait(
    rpc: &RpcClient,
    payer: &Keypair,
    cu_samples: usize,
    cu_price: u64,
    max_timeout: Duration,
) -> anyhow::Result<Metric> {
    trace!("Get latest blockhash and generate transactions");
    let hash = rpc.get_latest_blockhash().await.map_err(|err| {
        log::error!("Error get latest blockhash : {err:?}");
        err
    })?;
    let txs = generate_cu_requested_transactions(cu_samples, cu_price, payer, hash);

    trace!("Sending {} transactions in bulk ..", txs.len());
    let tx_and_confirmations_from_rpc: Vec<(Signature, ConfirmationResponseFromRpc)> =
        send_and_confirm_bulk_transactions(rpc, &txs, max_timeout)
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
                    "Signature {} failed to get sent via RPC: {:?}",
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

    // let average_confirmation_time_ms = if tx_confirmed > 0 {
    //     sum_confirmation_time.as_secs_f32() * 1000.0 / tx_confirmed as f32
    // } else {
    //     0.0
    // };
    // let average_slot_confirmation_time = if tx_confirmed > 0 {
    //     sum_slot_confirmation_time as f32 / tx_confirmed as f32
    // } else {
    //     0.0
    // };

    Ok(Metric::default())
}

// fn calc_stats(stats: &[Metric]) -> Metric {
//     let len = stats.len();

//     avg
// }

const CU_LIMITS: [u64; 9] = [5_000, 10_000, 20_000, 40_000, 80_000, 160_000, 320_000, 640_000, 1_280_000];

fn generate_cu_requested_transactions(
    cu_samples: usize,
    cu_price: u64,
    payer: &Keypair,
    blockhash: Hash,
) -> Vec<VersionedTransaction> {
    let mut txns = vec![];

    for i in 0..cu_samples {
        let cu_limit = CU_LIMITS[i];
        
        // start at 1750 CU, each additional byte of memo is ~300-400 CU
        let message_bytes = ((cu_limit - 1750) / 400) as usize;
        let message = vec![71u8; message_bytes];

        let cu_budget_ix: Instruction = ComputeBudgetInstruction::set_compute_unit_price(cu_price);
        let cu_limit_ix: Instruction =
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit as u32);
        let memo = Pubkey::from_str(MEMO_PROGRAM_ID).unwrap();
        let instruction = Instruction::new_with_bytes(memo, &message, vec![]);
        let message = v0::Message::try_compile(
            &payer.pubkey(),
            &[cu_budget_ix, cu_limit_ix, instruction],
            &[],
            blockhash,
        )
        .unwrap();
        let versioned_message = solana_sdk::message::VersionedMessage::V0(message);
        let tx = VersionedTransaction::try_new(versioned_message, &[&payer]).unwrap();
        txns.push(tx);
    }
    txns
}

