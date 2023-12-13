use std::sync::Arc;
use geyser_grpc_connector::grpcmultiplex_fastestwins::{create_multiplex, create_multiplex_confirmed, GrpcSourceConfig};
// use solana_rpc_client::nonblocking::rpc_client::RpcClient;
// use solana_lite_rpc_cluster_endpoints::endpoint_stremers::EndpointStreaming;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_lite_rpc_core::types::BlockStream;

pub fn create_grpc_multiplex_subscription(
    grpc_addr: String,
    grpc_x_token: Option<String>,
    expected_grpc_version: String,
) -> anyhow::Result<(BlockStream, AnyhowJoinHandle)> {

    let (block_sx, blocks_notifier) = tokio::sync::broadcast::channel(1000);

    let grpc_addr_mainnet_ams81 = "http://202.8.8.12:10000".to_string();

    let ams81_config = GrpcSourceConfig::new("mangoams81".to_string(), grpc_addr_mainnet_ams81, None);

    let jh_multiplex = create_multiplex_confirmed(
        vec![ams81_config],
        block_sx);

    // FIXME -empty
    let (blobb, blocksstuff_notifier) = tokio::sync::broadcast::channel(1000);

    Ok((blocksstuff_notifier, jh_multiplex))
}
