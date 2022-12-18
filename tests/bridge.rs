use std::str::FromStr;
use std::time::Duration;

use reqwest::Url;
use solana_client::rpc_response::RpcVersionInfo;
use solana_sdk::{
    message::Message, native_token::LAMPORTS_PER_SOL, pubkey::Pubkey, signature::Keypair,
    signer::Signer, system_instruction, transaction::Transaction,
};

use lite_rpc::{bridge::LiteBridge, encoding::BinaryEncoding, rpc::SendTransactionParams};

const RPC_ADDR: &str = "http://127.0.0.1:8899";
const WS_ADDR: &str = "ws://127.0.0.1:8900";

#[tokio::test]
async fn get_version() {
    let light_bridge = LiteBridge::new(Url::from_str(RPC_ADDR).unwrap(), WS_ADDR)
        .await
        .unwrap();

    let RpcVersionInfo {
        solana_core,
        feature_set,
    } = light_bridge.get_version();
    let version_crate = solana_version::Version::default();

    assert_eq!(solana_core, version_crate.to_string());
    assert_eq!(feature_set.unwrap(), version_crate.feature_set);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_send_transaction() {
    let light_bridge = LiteBridge::new(Url::from_str(RPC_ADDR).unwrap(), WS_ADDR)
        .await
        .unwrap();

    let payer = Keypair::new();

    light_bridge
        .tpu_client
        .rpc_client()
        .request_airdrop(&payer.pubkey(), LAMPORTS_PER_SOL * 2)
        .await
        .unwrap();

    std::thread::sleep(Duration::from_secs(2));

    let to_pubkey = Pubkey::new_unique();
    let instruction = system_instruction::transfer(&payer.pubkey(), &to_pubkey, LAMPORTS_PER_SOL);

    let message = Message::new(&[instruction], Some(&payer.pubkey()));

    let blockhash = light_bridge
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
        light_bridge
            .send_transaction(SendTransactionParams(tx, Default::default()))
            .await
            .unwrap(),
        encoded_signature
    );

    std::thread::sleep(Duration::from_secs(5));

    let mut passed = false;

    for _ in 0..100 {
        passed = light_bridge
            .tpu_client
            .rpc_client()
            .confirm_transaction(&signature)
            .await
            .unwrap();

        std::thread::sleep(Duration::from_millis(100));
    }

    passed.then_some(()).unwrap();
}
