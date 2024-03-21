use std::path::Path;
use std::time::Duration;

use crate::metrics::{PingThing, PingThingTxType};
use crate::{create_memo_tx, create_rng, BenchmarkTransactionParams, Rng8};
use log::{debug, info};
use solana_lite_rpc_util::obfuscate_rpcurl;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::{RpcSendTransactionConfig, RpcTransactionConfig};
use solana_sdk::signature::{read_keypair_file, Signature, Signer};
use solana_sdk::transaction::Transaction;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};
use solana_transaction_status::UiTransactionEncoding;
use std::ops::Add;
use tokio::time::{sleep, Instant};
use tracing::error;

#[derive(Default)]
pub struct ConfirmationSlotResult {
    pub signature: Signature,
    pub slot_sent: u64,
    pub slot_landed: u64,
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
    max_timeout_ms: u64,
    num_of_runs: usize,
    maybe_ping_thing: Option<PingThing>,
) -> anyhow::Result<()> {
    info!("START BENCHMARK: confirmation_slot");
    info!("RPC A: {}", obfuscate_rpcurl(&rpc_a_url));
    info!("RPC B: {}", obfuscate_rpcurl(&rpc_b_url));

    let mut rng = create_rng(None);
    let payer = read_keypair_file(payer_path).expect("payer file");
    info!("Payer: {}", payer.pubkey().to_string());

    for _ in 0..num_of_runs {
        let rpc_a = RpcClient::new(rpc_a_url.clone());
        let rpc_b = RpcClient::new(rpc_b_url.clone());
        // measure network time to reach the respective RPC endpoints,
        // used to mitigate the difference in distance by delaying the txn sending
        let time_a = rpc_roundtrip_duration(&rpc_a).await?.as_secs_f64();
        let time_b = rpc_roundtrip_duration(&rpc_b).await?.as_secs_f64();

        debug!("{} (A) latency: {}", obfuscate_rpcurl(&rpc_a.url()), time_a);
        debug!("{} (B) latency: {}", obfuscate_rpcurl(&rpc_b.url()), time_b);

        let rpc_a_tx = create_tx(&rpc_a, &payer, &mut rng, &tx_params).await?;
        let rpc_b_tx = create_tx(&rpc_b, &payer, &mut rng, &tx_params).await?;

        let one_way_delay = (time_a - time_b).abs() / 2.0;
        let (a_delay, b_delay) = if time_a > time_b {
            (0f64, one_way_delay)
        } else {
            (one_way_delay, 0f64)
        };

        debug!("A delay: {}, B delay: {}", a_delay, b_delay);

        let a_task = tokio::spawn(async move {
            sleep(Duration::from_secs_f64(a_delay)).await;
            send_and_confirm_transaction(&rpc_a, rpc_a_tx, max_timeout_ms)
                .await
                .unwrap_or_else(|e| {
                    error!("Failed to send_and_confirm_transaction for A: {}", e);
                    ConfirmationSlotResult::default()
                })
        });

        let b_task = tokio::spawn(async move {
            sleep(Duration::from_secs_f64(b_delay)).await;
            send_and_confirm_transaction(&rpc_b, rpc_b_tx, max_timeout_ms)
                .await
                .unwrap_or_else(|e| {
                    error!("Failed to send_and_confirm_transaction for B: {}", e);
                    ConfirmationSlotResult::default()
                })
        });

        let (a, b) = tokio::join!(a_task, b_task);
        let a_result = a?;
        let b_result = b?;

        if let Some(ref ping_thing) = maybe_ping_thing {
            submit_ping_thing_stats(&a_result, &ping_thing).await?;
            submit_ping_thing_stats(&b_result, &ping_thing).await?;
        };

        info!(
            "a_slot: {}, b_slot: {}\n",
            a_result.slot_landed, b_result.slot_landed
        );
    }

    Ok(())
}

async fn create_tx(
    rpc: &RpcClient,
    payer: &Keypair,
    rng: &mut Rng8,
    tx_params: &BenchmarkTransactionParams,
) -> anyhow::Result<Transaction> {
    let hash = rpc.get_latest_blockhash().await?;

    Ok(create_memo_tx(payer, hash, rng, tx_params))
}

async fn send_and_confirm_transaction(
    rpc: &RpcClient,
    tx: Transaction,
    timeout_ms: u64,
) -> anyhow::Result<ConfirmationSlotResult> {
    let send_config = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: None,
        encoding: None,
        max_retries: Some(3),
        min_context_slot: None,
    };
    let slot_sent = rpc
        .get_slot_with_commitment(CommitmentConfig::confirmed())
        .await?;
    let signature = rpc
        .send_transaction_with_config(&tx, send_config)
        .await
        .map_err(|err| anyhow::anyhow!("error sending transaction: {:?}", err))?;

    let started_at = Instant::now();
    let mut throttle_barrier = Instant::now();
    loop {
        if started_at.elapsed() >= Duration::from_millis(timeout_ms) {
            return Ok(ConfirmationSlotResult::default()); // Timeout occurred
        }
        let confirmed = rpc
            .confirm_transaction_with_commitment(&signature, CommitmentConfig::confirmed())
            .await
            .map_err(|err| {
                anyhow::anyhow!("error confirming transaction {:?}: {:?}", &signature, err)
            })?;
        if confirmed.value {
            break;
        }
        info!("try confirm: {:?}", throttle_barrier); // TOOD: delete this comment
        tokio::time::sleep_until(throttle_barrier).await;
        throttle_barrier = Instant::now().add(Duration::from_millis(1000));
    }

    let confirmation_time = started_at.elapsed();

    let fetch_config = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Json),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };
    let transaction = rpc
        .get_transaction_with_config(&signature, fetch_config)
        .await?;

    Ok(ConfirmationSlotResult {
        signature,
        slot_sent,
        slot_landed: transaction.slot,
        confirmation_time,
    })
}

pub async fn rpc_roundtrip_duration(rpc: &RpcClient) -> anyhow::Result<Duration> {
    let started_at = Instant::now();
    rpc.get_health().await?;
    let duration = started_at.elapsed();
    Ok(duration)
}

async fn submit_ping_thing_stats(
    result: &ConfirmationSlotResult,
    ping_thing: &PingThing,
) -> anyhow::Result<()> {
    ping_thing
        .submit_confirmed_stats(
            result.confirmation_time,
            result.signature,
            PingThingTxType::Memo,
            true,
            result.slot_sent,
            result.slot_landed,
        )
        .await
}
