use std::sync::Arc;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::commitment_config::CommitmentConfig;
use crate::{endpoint_stremers::EndpointStreaming, rpc_polling::{poll_slots::poll_slots, poll_blocks::poll_block, vote_accounts_and_cluster_info_polling::poll_vote_accounts_and_cluster_info}};

pub fn create_json_rpc_polling_subscription(
    rpc_client: Arc<RpcClient>,
) -> anyhow::Result<(EndpointStreaming, Vec<AnyhowJoinHandle>)> {
    let (slot_sx, slot_notifier) = tokio::sync::broadcast::channel(10);
    let (block_sx, blocks_notifier) = tokio::sync::broadcast::channel(10);
    let (cluster_info_sx, cluster_info_notifier) = tokio::sync::broadcast::channel(10);
    let (va_sx, vote_account_notifier) = tokio::sync::broadcast::channel(10);

    let mut endpoint_tasks = vec![];
    let slot_polling_task = tokio::spawn(poll_slots(rpc_client.clone(), CommitmentConfig::processed(), slot_sx));
    endpoint_tasks.push(slot_polling_task);

    let mut block_polling_tasks = poll_block(rpc_client.clone(), block_sx, slot_notifier.resubscribe());
    endpoint_tasks.append(&mut block_polling_tasks);

    let cluster_info_polling = poll_vote_accounts_and_cluster_info(rpc_client.clone(), cluster_info_sx, va_sx);
    endpoint_tasks.push(cluster_info_polling);

    let streamers = EndpointStreaming{
        blocks_notifier,
        slot_notifier,
        cluster_info_notifier,
        vote_account_notifier,
    };
    Ok((streamers, endpoint_tasks))
}
