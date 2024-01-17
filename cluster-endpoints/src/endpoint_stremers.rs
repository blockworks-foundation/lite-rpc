use solana_lite_rpc_blocks_processing::produced_block::BlockStream;
use solana_lite_rpc_core::{types::{ClusterInfoStream, VoteAccountStream}, structures::slot_notification::SlotStream};
pub struct EndpointStreaming {
    pub blocks_notifier: BlockStream,
    pub slot_notifier: SlotStream,
    pub vote_account_notifier: VoteAccountStream,
    pub cluster_info_notifier: ClusterInfoStream,
}
