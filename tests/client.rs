use std::sync::Arc;

use bench_utils::helpers::BenchHelper;
use log::info;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_client::SerializableTransaction};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::native_token::LAMPORTS_PER_SOL;

use lite_client::{LiteClient, LOCAL_LIGHT_RPC_ADDR};
use simplelog::*;

const AMOUNT: usize = 5;

#[tokio::test]
async fn send_and_confirm_tx() {
    TermLogger::init(
        LevelFilter::Info,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
    .unwrap();

    let lite_client = Arc::new(LiteClient(RpcClient::new(LOCAL_LIGHT_RPC_ADDR.to_string())));
    let bench_helper = BenchHelper::new(lite_client.clone());

    let funded_payer = bench_helper
        .new_funded_payer(LAMPORTS_PER_SOL * 2)
        .await
        .unwrap();

    let txs = bench_helper
        .generate_txs(AMOUNT, &funded_payer)
        .await
        .unwrap();

    info!("Sending and Confirming {AMOUNT} tx(s)");

    for tx in &txs {
        lite_client.send_transaction(tx).await.unwrap();
        info!("Tx {}", &tx.signatures[0]);
    }

    for tx in &txs {
        let sig = tx.get_signature();
        info!("Confirming {sig}");
        bench_helper
            .wait_till_commitment(sig, CommitmentConfig::confirmed())
            .await;
    }

    info!("Sent and Confirmed {AMOUNT} tx(s)");
}
