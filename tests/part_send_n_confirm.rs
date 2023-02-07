use bench::helpers::BenchHelper;
use lite_rpc::{DEFAULT_LITE_RPC_ADDR, DEFAULT_RPC_ADDR};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

#[tokio::test]
async fn part_send_n_confirm() -> anyhow::Result<()> {
    let rpc_client = RpcClient::new(DEFAULT_RPC_ADDR.to_string());
    let lite_rpc_client = RpcClient::new(DEFAULT_LITE_RPC_ADDR.to_string());

    send_and_confirm_memo(&rpc_client, &lite_rpc_client).await?;
    send_and_confirm_memo(&lite_rpc_client, &rpc_client).await?;

    Ok(())
}

pub async fn send_and_confirm_memo(
    send_rpc: &RpcClient,
    confirm_rpc: &RpcClient,
) -> anyhow::Result<()> {
    let payer = BenchHelper::get_payer().await?;
    let blockhash = send_rpc.get_latest_blockhash().await?;

    let memo_tx = BenchHelper::create_memo_tx(b"hi", &payer, blockhash);
    let memo_sig = send_rpc.send_transaction(&memo_tx).await?;

    confirm_rpc.confirm_transaction(&memo_sig).await?;

    println!("{memo_sig}");

    Ok(())
}
