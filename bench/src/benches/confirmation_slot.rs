use std::path::Path;
use std::time::Duration;

use crate::tx_size::TxSize;
use crate::{create_memo_tx, create_rng, Rng8};
use anyhow::Context;
use log::{info, warn};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::{RpcSendTransactionConfig, RpcTransactionConfig};
use solana_sdk::signature::{read_keypair_file, Signer};
use solana_sdk::transaction::Transaction;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};
use solana_transaction_status::UiTransactionEncoding;
use tokio::time::{sleep, Instant};

/// TC1 -- Send 2 txs to separate RPCs and compare confirmation slot.
/// The benchmark attempts to minimize the effect of real-world distance and synchronize the time that each transaction reaches the RPC.
/// This is achieved via delayed sending the transaction to the nearer RPC.
/// Delay time is calculated as half of the difference in duration of [getHealth](https://solana.com/docs/rpc/http/gethealth) calls to both RPCs.
pub async fn confirmation_slot(
    payer_path: &Path,
    rpc_a_url: String,
    rpc_b_url: String,
    tx_size: TxSize,
    cu_price_micro_lamports: u64,
) -> anyhow::Result<()> {
    warn!("THIS IS WORK IN PROGRESS");

    let rpc_a = RpcClient::new(rpc_a_url);
    info!("RPC A: {}", rpc_a.url());

    let rpc_b = RpcClient::new(rpc_b_url);
    info!("RPC B: {}", rpc_b.url());

    let mut rng = create_rng(None);
    let payer = read_keypair_file(payer_path).expect("payer file");
    info!("Payer: {}", payer.pubkey().to_string());

    // TODO: loop here

    let time_a = rpc_roundtrip_duration(&rpc_a).await?.as_secs_f64();
    let time_b = rpc_roundtrip_duration(&rpc_b).await?.as_secs_f64();

    info!("{} (A) latency: {}", rpc_a.url(), time_a);
    info!("{} (B) latency: {}", rpc_b.url(), time_b);

    let rpc_a_tx = create_tx(&rpc_a, &payer, &mut rng, tx_size, cu_price_micro_lamports).await?;
    let rpc_b_tx = create_tx(&rpc_b, &payer, &mut rng, tx_size, cu_price_micro_lamports).await?;

    let half_round_trip = (time_a - time_b).abs() / 2.0;
    let (a_delay, b_delay) = if time_a > time_b {
        (0f64, half_round_trip)
    } else {
        (half_round_trip, 0f64)
    };

    info!("A delay: {}, B delay: {}", a_delay, b_delay);

    let a_task = tokio::spawn(async move {
        sleep(Duration::from_secs_f64(a_delay)).await;
        send_transaction_and_get_slot(&rpc_a, rpc_a_tx)
            .await
            .unwrap()
    });

    let b_task = tokio::spawn(async move {
        sleep(Duration::from_secs_f64(b_delay)).await;
        send_transaction_and_get_slot(&rpc_b, rpc_b_tx)
            .await
            .unwrap()
    });

    let (a_slot, b_slot) = tokio::join!(a_task, b_task);

    info!("a_slot: {}", a_slot?);
    info!("b_slot: {}", b_slot?);

    Ok(())
}

async fn create_tx(
    rpc: &RpcClient,
    payer: &Keypair,
    rng: &mut Rng8,
    tx_size: TxSize,
    cu_price_micro_lamports: u64,
) -> anyhow::Result<Transaction> {
    let hash = rpc.get_latest_blockhash().await?;

    Ok(create_memo_tx(
        payer,
        hash,
        rng,
        tx_size,
        cu_price_micro_lamports,
    ))
}

async fn send_transaction_and_get_slot(rpc: &RpcClient, tx: Transaction) -> anyhow::Result<u64> {
    let send_config = RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: None,
        encoding: None,
        max_retries: Some(3),
        min_context_slot: None,
    };
    let signature = rpc.send_transaction_with_config(&tx, send_config).await?;

    loop {
        let confirmed = rpc
            .confirm_transaction_with_commitment(&signature, CommitmentConfig::confirmed())
            .await?;
        if confirmed.value {
            break;
        }
    }

    let fetch_config = RpcTransactionConfig {
        encoding: Some(UiTransactionEncoding::Json),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };
    let transaction = rpc
        .get_transaction_with_config(&signature, fetch_config)
        .await
        .context("Failed to fetch transaction")?;
    Ok(transaction.slot)
}

pub async fn rpc_roundtrip_duration(rpc: &RpcClient) -> anyhow::Result<Duration> {
    let start = Instant::now();
    rpc.get_health().await?;
    let duration = start.elapsed();
    Ok(duration)
}
