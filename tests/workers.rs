use std::{sync::Arc, time::Duration};

use bench::helpers::BenchHelper;
use futures::future::try_join_all;
use lite_rpc::{
    encoding::BinaryEncoding,
    tpu_manager::TpuManager,
    workers::{BlockListener, TxSender},
    DEFAULT_LITE_RPC_ADDR, DEFAULT_RPC_ADDR, DEFAULT_TX_BATCH_INTERVAL_MS, DEFAULT_TX_BATCH_SIZE,
    DEFAULT_WS_ADDR,
};
use solana_client::nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient};

use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::TransactionConfirmationStatus;

#[tokio::test]
async fn send_and_confirm_txs() {
    let rpc_client = Arc::new(RpcClient::new(DEFAULT_RPC_ADDR.to_string()));
    let lite_client = Arc::new(RpcClient::new(DEFAULT_LITE_RPC_ADDR.to_string()));
    let bench_helper = BenchHelper::new(lite_client.clone());

    let tpu_client = Arc::new(
        TpuManager::new(
            rpc_client.clone(),
            DEFAULT_WS_ADDR.into(),
            Default::default(),
        )
        .await
        .unwrap(),
    );

    let pub_sub_client = Arc::new(PubsubClient::new(DEFAULT_WS_ADDR).await.unwrap());

    let tx_sender = TxSender::new(tpu_client);

    let block_listener = BlockListener::new(
        pub_sub_client.clone(),
        rpc_client.clone(),
        tx_sender.clone(),
        CommitmentConfig::confirmed(),
    )
    .await
    .unwrap();

    let services = try_join_all(vec![
        block_listener.clone().listen(None),
        tx_sender.clone().execute(
            DEFAULT_TX_BATCH_SIZE,
            Duration::from_millis(DEFAULT_TX_BATCH_INTERVAL_MS),
        ),
    ]);

    let confirm = tokio::spawn(async move {
        let funded_payer = bench_helper.get_payer().await.unwrap();

        let blockhash = rpc_client.get_latest_blockhash().await.unwrap();

        let tx = bench_helper.create_transaction(&funded_payer, blockhash);
        let sig = tx.signatures[0];
        let tx = BinaryEncoding::Base58.encode(bincode::serialize(&tx).unwrap());
        let sig = sig.to_string();

        tx_sender
            .enqnueue_tx(sig.clone(), tx.as_bytes().to_vec())
            .await;

        for _ in 0..2 {
            let tx_status = tx_sender.txs_sent.get(&sig).unwrap();

            if let Some(tx_status) = &tx_status.value().status {
                if tx_status.confirmation_status() == TransactionConfirmationStatus::Confirmed {
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
