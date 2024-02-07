use crate::rpc_polling::vote_accounts_and_cluster_info_polling::{
    poll_cluster_info, poll_vote_accounts,
};
use crate::{
    endpoint_stremers::EndpointStreaming,
    rpc_polling::{poll_blocks::poll_block, poll_slots::poll_slots},
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::commitment_config::CommitmentConfig;
use std::sync::Arc;

pub fn create_json_rpc_polling_subscription(
    rpc_client: Arc<RpcClient>,
    num_parallel_tasks: usize,
) -> anyhow::Result<(EndpointStreaming, Vec<AnyhowJoinHandle>)> {
    let (slot_sx, slot_notifier) = tokio::sync::broadcast::channel(16);
    let (block_sx, blocks_notifier) = tokio::sync::broadcast::channel(16);
    let (cluster_info_sx, cluster_info_notifier) = tokio::sync::broadcast::channel(16);
    let (va_sx, vote_account_notifier) = tokio::sync::broadcast::channel(16);

    let mut endpoint_tasks =
        poll_slots(rpc_client.clone(), CommitmentConfig::processed(), slot_sx)?;

    let mut block_polling_tasks = poll_block(
        rpc_client.clone(),
        block_sx,
        slot_notifier.resubscribe(),
        num_parallel_tasks,
    );
    endpoint_tasks.append(&mut block_polling_tasks);

    let cluster_info_polling = poll_cluster_info(rpc_client.clone(), cluster_info_sx);
    endpoint_tasks.push(cluster_info_polling);

    let vote_accounts_polling = poll_vote_accounts(rpc_client.clone(), va_sx);
    endpoint_tasks.push(vote_accounts_polling);

    let streamers = EndpointStreaming {
        blocks_notifier,
        slot_notifier,
        cluster_info_notifier,
        vote_account_notifier,
    };
    Ok((streamers, endpoint_tasks))
}
