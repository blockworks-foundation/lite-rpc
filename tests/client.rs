use log::info;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_client::SerializableTransaction};
use solana_sdk::native_token::LAMPORTS_PER_SOL;

use bench_utils::helpers::{generate_txs, new_funded_payer, wait_till_confirmed};
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

    let lite_client = LiteClient(RpcClient::new(LOCAL_LIGHT_RPC_ADDR.to_string()));
    let funded_payer = new_funded_payer(&lite_client, LAMPORTS_PER_SOL * 2)
        .await
        .unwrap();

    let txs = generate_txs(AMOUNT, &lite_client.0, &funded_payer)
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
        wait_till_confirmed(&lite_client, sig).await;
    }

    info!("Sent and Confirmed {AMOUNT} tx(s)");
}
