use std::path::Path;
use std::time::Duration;

use crate::benches::rpc_interface::{
    create_rpc_client, send_and_confirm_bulk_transactions, ConfirmationResponseFromRpc,
};
use crate::metrics::PingThing;
use crate::{create_memo_tx, create_rng, BenchmarkTransactionParams, Rng8};
use anyhow::anyhow;
use log::{debug, info, warn};
use solana_lite_rpc_util::obfuscate_rpcurl;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::{read_keypair_file, Signature, Signer};
use solana_sdk::transaction::VersionedTransaction;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};
use tokio::time::{sleep, Instant};
use url::Url;

#[derive(Clone, Copy, Debug, Default)]
pub struct Metric {
    pub txs_sent: u64,
    pub txs_confirmed: u64,
    pub txs_un_confirmed: u64,
    pub average_confirmation_time_ms: f64,
    pub average_time_to_send_txs: f64,
}

#[derive(Clone)]
pub enum ConfirmationSlotResult {
    Success(ConfirmationSlotSuccess),
}

#[derive(Clone)]
pub struct ConfirmationSlotSuccess {
    pub slot_sent: u64,
    pub slot_confirmed: u64,
    pub confirmation_time: Duration,
}

#[allow(clippy::too_many_arguments)]
/// TC1 -- Send 2 txs to separate RPCs and compare confirmation slot.
/// The benchmark attempts to minimize the effect of real-world distance and synchronize the time that each transaction reaches the RPC.
/// This is achieved by delaying submission of the transaction to the "nearer" RPC.
/// Delay time is calculated as half of the difference in duration of [getHealth](https://solana.com/docs/rpc/http/gethealth) calls to both RPCs.
pub async fn confirmation_slot(
    payer_path: &Path,
    rpc_a_url: String,
    rpc_b_url: String,
    tx_params: BenchmarkTransactionParams,
    max_timeout: Duration,
    num_of_runs: usize,
    _maybe_ping_thing: Option<PingThing>,
) -> anyhow::Result<()> {
    info!(
        "START BENCHMARK: confirmation_slot (prio_fees={})",
        tx_params.cu_price_micro_lamports
    );
    warn!("THIS IS WORK IN PROGRESS");
    info!("RPC A: {}", obfuscate_rpcurl(&rpc_a_url));
    info!("RPC B: {}", obfuscate_rpcurl(&rpc_b_url));

    let rpc_a_url =
        Url::parse(&rpc_a_url).map_err(|e| anyhow!("Failed to parse RPC A URL: {}", e))?;
    let rpc_b_url =
        Url::parse(&rpc_b_url).map_err(|e| anyhow!("Failed to parse RPC B URL: {}", e))?;

    let mut rng = create_rng(None);
    let payer = read_keypair_file(payer_path).expect("payer file");
    info!("Payer: {}", payer.pubkey().to_string());
    // let mut ping_thing_tasks = vec![];

    for _ in 0..num_of_runs {
        let rpc_a = create_rpc_client(&rpc_a_url);
        let rpc_b = create_rpc_client(&rpc_b_url);
        // measure network time to reach the respective RPC endpoints,
        // used to mitigate the difference in distance by delaying the txn sending
        let time_a = rpc_roundtrip_duration(&rpc_a).await?.as_secs_f64();
        let time_b = rpc_roundtrip_duration(&rpc_b).await?.as_secs_f64();

        debug!("(A) rpc network latency: {}", time_a);
        debug!("(B) rpc network latency: {}", time_b);

        let rpc_a_tx = create_tx(&rpc_a, &payer, &mut rng, &tx_params).await?;
        let rpc_b_tx = create_tx(&rpc_b, &payer, &mut rng, &tx_params).await?;

        let one_way_delay = (time_a - time_b).abs() / 2.0;
        let (a_delay, b_delay) = if time_a > time_b {
            (0f64, one_way_delay)
        } else {
            (one_way_delay, 0f64)
        };

        debug!("A delay: {}s, B delay: {}s", a_delay, b_delay);

        let a_task = tokio::spawn(async move {
            sleep(Duration::from_secs_f64(a_delay)).await;
            debug!("(A) sending tx {}", rpc_a_tx.signatures[0]);
            send_and_confirm_transaction(&rpc_a, rpc_a_tx, max_timeout).await
        });

        let b_task = tokio::spawn(async move {
            sleep(Duration::from_secs_f64(b_delay)).await;
            debug!("(B) sending tx {}", rpc_b_tx.signatures[0]);
            send_and_confirm_transaction(&rpc_b, rpc_b_tx, max_timeout).await
        });

        let (a, b) = tokio::join!(a_task, b_task);
        // only continue if both paths suceed
        let a_result: ConfirmationResponseFromRpc = a??;
        let b_result: ConfirmationResponseFromRpc = b??;

        if let (
            ConfirmationResponseFromRpc::Success(a_slot_sent, a_slot_confirmed, _, _),
            ConfirmationResponseFromRpc::Success(b_slot_sent, b_slot_confirmed, _, _),
        ) = (a_result, b_result)
        {
            info!(
                "txn A landed after {} slots",
                a_slot_confirmed - a_slot_sent
            );
            info!(
                "txn B landed after {} slots",
                b_slot_confirmed - b_slot_sent
            );
        }

        // if let Some(ping_thing) = maybe_ping_thing.clone() {
        //     ping_thing_tasks.push(tokio::spawn(async move {
        //         submit_ping_thing_stats(&a_result, &ping_thing)
        //             .await
        //             .unwrap();
        //         submit_ping_thing_stats(&b_result, &ping_thing)
        //             .await
        //             .unwrap();
        //     }));
        // };
    }

    // futures::future::join_all(ping_thing_tasks).await;

    Ok(())
}

async fn create_tx(
    rpc: &RpcClient,
    payer: &Keypair,
    rng: &mut Rng8,
    tx_params: &BenchmarkTransactionParams,
) -> anyhow::Result<VersionedTransaction> {
    let (blockhash, _) = rpc
        .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
        .await?;

    Ok(create_memo_tx(payer, blockhash, rng, tx_params))
}

async fn send_and_confirm_transaction(
    rpc: &RpcClient,
    tx: VersionedTransaction,
    max_timeout: Duration,
) -> anyhow::Result<ConfirmationResponseFromRpc> {
    let result_vec: Vec<(Signature, ConfirmationResponseFromRpc)> =
        send_and_confirm_bulk_transactions(rpc, &[tx], max_timeout).await?;
    assert_eq!(result_vec.len(), 1, "expected 1 result");
    let (_sig, confirmation_response) = result_vec.into_iter().next().unwrap();

    Ok(confirmation_response)
}

pub async fn rpc_roundtrip_duration(rpc: &RpcClient) -> anyhow::Result<Duration> {
    let started_at = Instant::now();
    rpc.get_health().await?;
    let duration = started_at.elapsed();
    Ok(duration)
}

// async fn submit_ping_thing_stats(
//     confirmation_info: &ConfirmationSlotResult,
//     ping_thing: &PingThing,
// ) -> anyhow::Result<()> {
//     match confirmation_info.result {
//         ConfirmationSlotResult::Timeout(_) => Ok(()),
//         ConfirmationSlotResult::Success(slot_landed) => {
//             ping_thing
//                 .submit_confirmed_stats(
//                     confirmation_info.confirmation_time,
//                     confirmation_info.signature,
//                     PingThingTxType::Memo,
//                     true,
//                     confirmation_info.slot_sent,
//                     slot_landed,
//                 )
//                 .await
//         }
//     }
// }
