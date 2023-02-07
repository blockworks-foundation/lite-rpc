use std::sync::Arc;

use bench::helpers::BenchHelper;
use lite_rpc::DEFAULT_LITE_RPC_ADDR;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

#[tokio::test]
async fn memo() {
    let rpc_client = Arc::new(RpcClient::new(DEFAULT_LITE_RPC_ADDR.to_owned()));
    let helper = BenchHelper::new(rpc_client);
    let payer = helper.get_payer().await.unwrap();
    let blockhash = helper.get_latest_blockhash().await.unwrap();

    let sig = helper
        .send_and_confirm_memo(b"hello", &payer, blockhash)
        .await
        .unwrap();

    println!("{sig}");
}
