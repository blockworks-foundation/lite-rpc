
use lite_rpc::DEFAULT_LITE_RPC_ADDR;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

#[tokio::test]
async fn blockhash() -> anyhow::Result<()> {
    let lite_rpc = RpcClient::new(DEFAULT_LITE_RPC_ADDR.to_string());

    let mut prev_blockhash = lite_rpc.get_latest_blockhash().await.unwrap();

    for _ in 0..5 {
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let blockhash = lite_rpc.get_latest_blockhash().await.unwrap();

        if prev_blockhash != blockhash {
            prev_blockhash = blockhash;
        } else {
            panic!("Blockhash didn't change in appx 500ms");
        }
    }

    Ok(())
}
