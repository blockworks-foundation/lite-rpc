use lite_rpc::{DEFAULT_LITE_RPC_ADDR, DEFAULT_RPC_ADDR};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;

#[tokio::test]
async fn diff_rpc() -> anyhow::Result<()> {
    let rpc_client = RpcClient::new(DEFAULT_RPC_ADDR.to_string());
    let lite_rpc_client = RpcClient::new(DEFAULT_LITE_RPC_ADDR.to_string());

    check_block_hash(&rpc_client, &lite_rpc_client, CommitmentConfig::confirmed()).await?;
    check_block_hash(&rpc_client, &lite_rpc_client, CommitmentConfig::finalized()).await?;

    Ok(())
}

async fn check_block_hash(
    rpc_client: &RpcClient,
    lite_rpc_client: &RpcClient,
    commitment_config: CommitmentConfig,
) -> anyhow::Result<()> {
    let rpc_blockhash = rpc_client
        .get_latest_blockhash_with_commitment(commitment_config)
        .await?;
    let lite_blockhash = lite_rpc_client
        .get_latest_blockhash_with_commitment(commitment_config)
        .await?;

    println!("{commitment_config:?} {rpc_blockhash:?} {lite_blockhash:?}");

    assert_eq!(rpc_blockhash.0, lite_blockhash.0);
    assert_eq!(rpc_blockhash.1, lite_blockhash.1);

    Ok(())
}
