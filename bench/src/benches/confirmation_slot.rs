use anyhow::Context;
use bench_lib::tx_size::TxSize;
use bench_lib::{create_memo_tx, create_rng, send_and_confirm_transactions, Rng8};
use log::info;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::{read_keypair_file, Signer};
use solana_sdk::transaction::Transaction;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};

/// send 2 txs (one via LiteRPC, one via Solana RPC) and compare confirmation slot (=slot distance)
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let lite_rpc_addr = "http://0.0.0.0:8890".to_string();
    let rpc_addr = "https://api.mainnet-beta.solana.com/".to_string();
    let payer_path = "/path/to/id.json";
    let tx_size = TxSize::Small;

    let lite_rpc = RpcClient::new(lite_rpc_addr.clone());
    let rpc = RpcClient::new(rpc_addr.clone());
    info!("Lite RPC: {}", lite_rpc_addr);
    info!("RPC: {}", rpc_addr);

    let mut rng = create_rng(None);
    let payer = read_keypair_file(&payer_path).unwrap();
    info!("Payer: {}", payer.pubkey().to_string());

    let rpc_tx = create_tx(&rpc, &payer, &mut rng, tx_size).await?;
    let lite_rpc_tx = create_tx(&lite_rpc, &payer, &mut rng, tx_size).await?;

    let (rpc_slot, lite_rpc_slot) = tokio::join!(
        send_transaction_and_get_slot(&rpc, rpc_tx),
        send_transaction_and_get_slot(&lite_rpc, lite_rpc_tx)
    );

    println!("rpc_slot: {}", rpc_slot?);
    println!("lite_rpc_slot: {}", lite_rpc_slot?);

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
