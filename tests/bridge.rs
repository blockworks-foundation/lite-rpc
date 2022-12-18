use std::str::FromStr;
use std::time::Duration;

use lite_rpc::{DEFAULT_RPC_ADDR, DEFAULT_WS_ADDR};
use reqwest::Url;
use solana_client::rpc_response::RpcVersionInfo;
use solana_sdk::{
    message::Message, native_token::LAMPORTS_PER_SOL, pubkey::Pubkey, signature::Keypair,
    signer::Signer, system_instruction, transaction::Transaction,
};

use lite_rpc::{bridge::LiteBridge, encoding::BinaryEncoding, rpc::SendTransactionParams};

#[tokio::test]
async fn get_version() {
    let lite_bridge = LiteBridge::new(Url::from_str(DEFAULT_RPC_ADDR).unwrap(), DEFAULT_WS_ADDR)
        .await
        .unwrap();

    let RpcVersionInfo {
        solana_core,
        feature_set,
    } = lite_bridge.get_version();
    let version_crate = solana_version::Version::default();

    assert_eq!(solana_core, version_crate.to_string());
    assert_eq!(feature_set.unwrap(), version_crate.feature_set);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_send_transaction() {
    let lite_bridge = LiteBridge::new(Url::from_str(DEFAULT_RPC_ADDR).unwrap(), DEFAULT_WS_ADDR)
        .await
        .unwrap();

    let payer = Keypair::new();

    lite_bridge
        .tpu_client
        .rpc_client()
        .request_airdrop(&payer.pubkey(), LAMPORTS_PER_SOL * 2)
        .await
        .unwrap();

    std::thread::sleep(Duration::from_secs(2));

    let to_pubkey = Pubkey::new_unique();
    let instruction = system_instruction::transfer(&payer.pubkey(), &to_pubkey, LAMPORTS_PER_SOL);

    let message = Message::new(&[instruction], Some(&payer.pubkey()));

    let blockhash = lite_bridge
        .tpu_client
        .rpc_client()
        .get_latest_blockhash()
        .await
        .unwrap();

    let tx = Transaction::new(&[&payer], message, blockhash);
    let signature = tx.signatures[0];
    let encoded_signature = BinaryEncoding::Base58.encode(signature);

    let tx = BinaryEncoding::Base58.encode(bincode::serialize(&tx).unwrap());

    assert_eq!(
        lite_bridge
            .send_transaction(SendTransactionParams(tx, Default::default()))
            .await
            .unwrap(),
        encoded_signature
    );

    std::thread::sleep(Duration::from_secs(5));

    let mut passed = false;

    for _ in 0..100 {
        passed = lite_bridge
            .tpu_client
            .rpc_client()
            .confirm_transaction(&signature)
            .await
            .unwrap();

        std::thread::sleep(Duration::from_millis(100));
    }

    passed.then_some(()).unwrap();
}
