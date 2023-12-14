use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use geyser_grpc_connector::grpcmultiplex_fastestwins::{create_multiplex, GrpcSourceConfig};
use log::info;
use tokio::spawn;
use tokio::sync::broadcast::Receiver;
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;
// use solana_rpc_client::nonblocking::rpc_client::RpcClient;
// use solana_lite_rpc_cluster_endpoints::endpoint_stremers::EndpointStreaming;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_lite_rpc_core::types::BlockStream;

pub async fn create_grpc_multiplex_subscription_finalized(
    grpc_addr: String,
    grpc_x_token: Option<String>,
    expected_grpc_version: String,
) -> anyhow::Result<(Receiver<Box<SubscribeUpdateBlock>>, AnyhowJoinHandle)> {

    let (block_sx, blocks_notifier) = tokio::sync::broadcast::channel(1000);

    let grpc_addr_mainnet_ams81 = "http://202.8.8.12:10000".to_string();

    let ams81_config = GrpcSourceConfig::new("mangoams81".to_string(), grpc_addr_mainnet_ams81, None);

    let jh_multiplex = create_multiplex(
        vec![ams81_config],
        CommitmentLevel::Finalized,
        block_sx
    ).await;

    Ok((blocks_notifier, jh_multiplex))
}
