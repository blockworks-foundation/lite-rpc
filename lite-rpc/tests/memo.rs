use bench::helpers::BenchHelper;
use lite_rpc::DEFAULT_LITE_RPC_ADDR;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

#[tokio::test]
async fn memo() -> anyhow::Result<()> {
    let lite_rpc = RpcClient::new(DEFAULT_LITE_RPC_ADDR.to_string());

    let payer = BenchHelper::get_payer().await.unwrap();
    let blockhash = lite_rpc.get_latest_blockhash().await.unwrap();

    let memo_tx = BenchHelper::create_memo_tx(b"hi", &payer, blockhash);
    let memo_sig = lite_rpc.send_transaction(&memo_tx).await.unwrap();

    lite_rpc.confirm_transaction(&memo_sig).await.unwrap();

    println!("{memo_sig}");

    Ok(())
}
