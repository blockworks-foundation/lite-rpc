use std::path::Path;

use crate::tx_size::TxSize;
use crate::{create_memo_tx, create_rng, send_and_confirm_transactions, Rng8};
use anyhow::Context;
use log::{info, warn};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::{read_keypair_file, Signer};
use solana_sdk::transaction::Transaction;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};

/// TC1 send 2 txs (one via LiteRPC, one via Solana RPC) and compare confirmation slot (=slot distance)
pub async fn confirmation_slot(
    payer_path: &Path,
    rpc_a_url: String,
    rpc_b_url: String,
    tx_size: TxSize,
) -> anyhow::Result<()> {
    warn!("THIS IS WORK IN PROGRESS");

    let rpc_a = RpcClient::new(rpc_a_url);
    info!("RPC A: {}", rpc_a.url());

    let rpc_b = RpcClient::new(rpc_b_url);
    info!("RPC B: {}", rpc_b.url());

    let mut rng = create_rng(None);
    let payer = read_keypair_file(payer_path).expect("payer file");
    info!("Payer: {}", payer.pubkey().to_string());

    let rpc_a_tx = create_tx(&rpc_a, &payer, &mut rng, tx_size).await?;
    let rpc_b_tx = create_tx(&rpc_b, &payer, &mut rng, tx_size).await?;

    let (rpc_slot, lite_rpc_slot) = tokio::join!(
        send_transaction_and_get_slot(&rpc_a, rpc_a_tx),
        send_transaction_and_get_slot(&rpc_b, rpc_b_tx)
    );

    info!("rpc_slot: {}", rpc_slot?);
    info!("lite_rpc_slot: {}", lite_rpc_slot?);

    Ok(())
}

async fn create_tx(
    rpc: &RpcClient,
    payer: &Keypair,
    rng: &mut Rng8,
    tx_size: TxSize,
) -> anyhow::Result<Transaction> {
    let hash = rpc.get_latest_blockhash().await?;

    Ok(create_memo_tx(payer, hash, rng, tx_size))
}

async fn send_transaction_and_get_slot(client: &RpcClient, tx: Transaction) -> anyhow::Result<u64> {
    let status = send_and_confirm_transactions(client, &[tx], CommitmentConfig::confirmed(), None)
        .await?
        .into_iter()
        .next()
        .unwrap()?
        .context("unable to confirm tx")?;

    Ok(status.slot)
}
