use solana_lite_rpc_core::streams::{
    BlockStream, ClusterInfoStream, SlotStream, VoteAccountStream,
};
pub struct EndpointStreaming {
    pub blocks_notifier: BlockStream,
    pub slot_notifier: SlotStream,
    pub vote_account_notifier: VoteAccountStream,
    pub cluster_info_notifier: ClusterInfoStream,
}
