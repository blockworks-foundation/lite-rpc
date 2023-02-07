use std::str::FromStr;
use std::sync::Arc;

use bench::helpers::BenchHelper;
use lite_rpc::DEFAULT_RPC_ADDR;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    instruction::Instruction, message::Message, pubkey::Pubkey, signer::Signer,
    transaction::Transaction,
};

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

#[tokio::test]
async fn memo() -> anyhow::Result<()> {
    let rpc_client = Arc::new(RpcClient::new(DEFAULT_RPC_ADDR.to_owned()));
    let blockhash = rpc_client.get_latest_blockhash().await.unwrap();

    let bench_helper = BenchHelper::new(rpc_client);
    let payer = bench_helper.get_payer().await.unwrap();

    let memo = Pubkey::from_str(MEMO_PROGRAM_ID).unwrap();

    let instruction = Instruction::new_with_bytes(memo, b"Hello", vec![]);
    let message = Message::new(&[instruction], Some(&payer.pubkey()));
    let tx = Transaction::new(&[&payer], message, blockhash);

    let sig = bench_helper
        .send_and_confirm_transaction(&tx)
        .await
        .unwrap();

    println!("{sig}");

    Ok(())
}
