use solana_lite_rpc_services::tpu_utils::quic_proxy_connection_manager::{
    QuicProxyConnectionManager, TpuNode,
};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::Transaction;
use std::net::SocketAddr;
use std::str::FromStr;

///
/// test with test-validator and quic-proxy
///
#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    send_test_to_proxy().await;
}

async fn send_test_to_proxy() {
    let tx = create_tx().await;

    // note: use the propaged TPU port - NOT port+6
    let tpu_address: SocketAddr = "127.0.0.1:1027".parse().unwrap();
    let proxy_address: SocketAddr = "127.0.0.1:11111".parse().unwrap();

    let tpu_node = TpuNode {
        tpu_address,
        // tpu pubkey is only used for logging
        tpu_identity: Pubkey::from_str("1111111jepwNWbYG87sgwnBbUJnQHrPiUJzMpqJXZ").unwrap(),
    };
    QuicProxyConnectionManager::send_simple_transactions(vec![tx], vec![tpu_node], proxy_address)
        .await
        .unwrap();
}

async fn create_tx() -> Transaction {
    let payer = Keypair::from_base58_string(
        "rKiJ7H5UUp3JR18kNyTF1XPuwPKHEM7gMLWHZPWP5djrW1vSjfwjhvJrevxF9MPmUmN9gJMLHZdLMgc9ao78eKr",
    );
    let payer_pubkey = payer.pubkey();

    let memo_ix = spl_memo::build_memo("Hello world".as_bytes(), &[&payer_pubkey]);

    Transaction::new_with_payer(&[memo_ix], Some(&payer_pubkey))
}
