use std::sync::Arc;

use bench::helpers::BenchHelper;
use lite_rpc::DEFAULT_LITE_RPC_ADDR;
use log::info;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client::rpc_client::SerializableTransaction;
use solana_sdk::commitment_config::CommitmentConfig;

const AMOUNT: usize = 5;

#[tokio::test]
async fn send_and_confirm_txs_get_signature_statuses() {
    tracing_subscriber::fmt::init();

    let rpc_client = Arc::new(RpcClient::new(DEFAULT_LITE_RPC_ADDR.to_string()));
    let bench_helper = BenchHelper::new(rpc_client.clone());

    let funded_payer = bench_helper.get_payer().await.unwrap();

    let txs = bench_helper
        .generate_txs(AMOUNT, &funded_payer)
        .await
        .unwrap();

    info!("Sending and Confirming {AMOUNT} tx(s)");

    for tx in &txs {
        rpc_client.send_transaction(tx).await.unwrap();
        info!("Tx {}", tx.get_signature());
    }

    for tx in &txs {
        let sig = tx.get_signature();
        info!("Confirming {sig}");
        bench_helper
            .wait_till_signature_status(sig, CommitmentConfig::confirmed())
            .await
            .unwrap();
    }

    info!("Sent and Confirmed {AMOUNT} tx(s)");
}

#[tokio::test]
async fn send_and_confirm_tx_rpc_client() {
    let rpc_client = Arc::new(RpcClient::new(DEFAULT_LITE_RPC_ADDR.to_string()));
    let bench_helper = BenchHelper::new(rpc_client.clone());

    let funded_payer = bench_helper.get_payer().await.unwrap();

    let tx = &bench_helper.generate_txs(1, &funded_payer).await.unwrap()[0];
    let sig = tx.get_signature();

    bench_helper.send_and_confirm_transaction(tx).await.unwrap();

    info!("Sent and Confirmed {sig}");
}
