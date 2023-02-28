use lite_rpc::DEFAULT_LITE_RPC_ADDR;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

const BLOCK_CHANGE_INTERVAL: u64 = 3000;

#[tokio::test]
async fn blockhash() -> anyhow::Result<()> {
    let lite_rpc = RpcClient::new(DEFAULT_LITE_RPC_ADDR.to_string());

    let mut prev_blockhash = lite_rpc.get_latest_blockhash().await.unwrap();

    for _ in 0..10 {
        tokio::time::sleep(tokio::time::Duration::from_millis(BLOCK_CHANGE_INTERVAL)).await;

        let blockhash = lite_rpc.get_latest_blockhash().await.unwrap();

        if prev_blockhash != blockhash {
            prev_blockhash = blockhash;
        } else {
            panic!("Blockhash didn't change in appx {BLOCK_CHANGE_INTERVAL}");
        }
    }

    Ok(())
}
