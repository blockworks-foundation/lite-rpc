use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use geyser_grpc_connector::grpcmultiplex_fastestwins::{create_multiplex, GrpcSourceConfig};
use log::{debug, info};
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::spawn;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::broadcast::Receiver;
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;
// use solana_rpc_client::nonblocking::rpc_client::RpcClient;
// use solana_lite_rpc_cluster_endpoints::endpoint_stremers::EndpointStreaming;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::types::BlockStream;
use crate::grpc_subscription::map_block_update;

pub async fn create_grpc_multiplex_subscription_finalized(
    commitment_config: CommitmentConfig,
    // grpc_addr: String,
    // grpc_x_token: Option<String>,
    // expected_grpc_version: String,
) -> anyhow::Result<(Receiver<ProducedBlock>, AnyhowJoinHandle)> {

    let (block_sx, mut blocks_notifier) = tokio::sync::broadcast::channel(1000);

    let grpc_addr_mainnet_ams81 = "http://202.8.8.12:10000".to_string();

    let ams81_config = GrpcSourceConfig::new("mangoams81".to_string(), grpc_addr_mainnet_ams81, None);

    let jh_multiplex = create_multiplex(
        vec![ams81_config],
        commitment_config,
        block_sx
    ).await;

    let (tx, multiplexed_finalized_blocks) = tokio::sync::broadcast::channel::<ProducedBlock>(1000);

    spawn(async move {
        'main_loop: while let Ok(block) = blocks_notifier.recv().await {
            info!("multiplex -> block #{} with {} txs", block.slot, block.transactions.len());

            let produced_block = map_block_update(*block, commitment_config);
            match tx.send(produced_block) {
                Ok(receivers) => {
                    debug!("sent block to {} receivers", receivers);
                }
                Err(send_error) => {
                    match send_error {
                        SendError(_) => {
                            info!("Stop sending blocks on stream - shutting down");
                            break 'main_loop;
                        }
                    }
                }
            };
        }
        panic!("forward task failed");
    });

    Ok((multiplexed_finalized_blocks, jh_multiplex))
}
