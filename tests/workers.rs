use std::{sync::Arc, time::Duration};

use bench::helpers::BenchHelper;
use futures::future::try_join_all;
use lite_rpc::{
    block_store::BlockStore,
    encoding::BinaryEncoding,
    workers::{tpu_utils::tpu_service::TpuService, BlockListener, TxSender},
    DEFAULT_RPC_ADDR, DEFAULT_WS_ADDR,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};
use solana_transaction_status::TransactionConfirmationStatus;
use tokio::sync::mpsc;

#[tokio::test]
async fn send_and_confirm_txs() {
    let rpc_client = Arc::new(RpcClient::new(DEFAULT_RPC_ADDR.to_string()));
    let current_slot = rpc_client.get_slot().await.unwrap();

    let tpu_service = TpuService::new(
        current_slot,
        32,
        Arc::new(Keypair::new()),
        rpc_client.clone(),
        DEFAULT_WS_ADDR.into(),
    )
    .await
    .unwrap();

    let tpu_client = Arc::new(tpu_service);

    let tx_sender = TxSender::new(tpu_client);
    let block_store = BlockStore::new(&rpc_client).await.unwrap();

    let block_listener = BlockListener::new(rpc_client.clone(), tx_sender.clone(), block_store);

    let (tx_send, tx_recv) = mpsc::channel(1024);

    let services = try_join_all(vec![
        block_listener
            .clone()
            .listen(CommitmentConfig::confirmed(), None),
        tx_sender.clone().execute(tx_recv, None),
    ]);

    let confirm = tokio::spawn(async move {
        let funded_payer = BenchHelper::get_payer().await.unwrap();

        let blockhash = rpc_client.get_latest_blockhash().await.unwrap();

        let tx = BenchHelper::create_transaction(&funded_payer, blockhash);
        let sig = tx.signatures[0];
        let tx = BinaryEncoding::Base58.encode(bincode::serialize(&tx).unwrap());
        let sig = sig.to_string();

        let _ = tx_send.send((sig.clone(), tx.as_bytes().to_vec(), 0)).await;

        for _ in 0..2 {
            let tx_status = tx_sender.txs_sent_store.get(&sig).unwrap();

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
