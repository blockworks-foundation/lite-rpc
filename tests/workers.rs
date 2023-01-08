use std::sync::Arc;
use std::time::Duration;

use bench_utils::helpers::BenchHelper;
use dashmap::DashMap;
use futures::future::try_join_all;
use lite_rpc::{
    encoding::BinaryEncoding,
    workers::{BlockListener, TxSender},
    DEFAULT_LITE_RPC_ADDR, DEFAULT_RPC_ADDR, DEFAULT_WS_ADDR,
};
use solana_client::nonblocking::{
    pubsub_client::PubsubClient, rpc_client::RpcClient, tpu_client::TpuClient,
};

use solana_sdk::{commitment_config::CommitmentConfig, native_token::LAMPORTS_PER_SOL};
use solana_transaction_status::TransactionConfirmationStatus;

#[tokio::test]
async fn send_and_confirm_txs() {
    let rpc_client = Arc::new(RpcClient::new(DEFAULT_RPC_ADDR.to_string()));
    let lite_client = Arc::new(RpcClient::new(DEFAULT_LITE_RPC_ADDR.to_string()));
    let bench_helper = BenchHelper::new(lite_client.clone());

    let tpu_client = Arc::new(
        TpuClient::new(rpc_client.clone(), DEFAULT_WS_ADDR, Default::default())
            .await
            .unwrap(),
    );

    let pub_sub_client = Arc::new(PubsubClient::new(DEFAULT_WS_ADDR).await.unwrap());
    let txs_sent = Arc::new(DashMap::new());

    let block_listener = BlockListener::new(
        pub_sub_client.clone(),
        rpc_client.clone(),
        txs_sent.clone(),
        CommitmentConfig::confirmed(),
    )
    .await
    .unwrap();

    let tx_sender = TxSender::new(tpu_client);

    let services = try_join_all(vec![
        block_listener.clone().listen(),
        tx_sender.clone().execute(),
    ]);

    let confirm = tokio::spawn(async move {
        let funded_payer = bench_helper
            .new_funded_payer(LAMPORTS_PER_SOL * 2)
            .await
            .unwrap();

        let blockhash = rpc_client.get_latest_blockhash().await.unwrap();

        let tx = bench_helper.create_transaction(&funded_payer, blockhash);
        let sig = tx.signatures[0];
        let tx = BinaryEncoding::Base58.encode(bincode::serialize(&tx).unwrap());

        tx_sender.enqnueue_tx(tx.as_bytes().to_vec());

        let sig = sig.to_string();

        for _ in 0..2 {
            let tx_status = txs_sent.get(&sig).unwrap();

            if let Some(tx_status) = tx_status.value() {
                if tx_status.confirmation_status()
                    == TransactionConfirmationStatus::Confirmed
                {
                    return;
                }
            }

            tokio::time::sleep(Duration::from_millis(800)).await;
        }

        panic!("Tx {sig} not confirmed in 1600ms");
    });

    tokio::select! {
        _ = services => {
            panic!("Services stopped unexpectedly")
        },
        _ = confirm => {}
    }
}
