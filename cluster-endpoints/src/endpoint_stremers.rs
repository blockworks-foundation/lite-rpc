use solana_lite_rpc_core::types::{
    BlockInfoStream, BlockStream, ClusterInfoStream, SlotStream, VoteAccountStream,
};

/// subscribers to broadcast channels should assume that channels are not getting closed unless the system is shutting down
pub struct EndpointStreaming {
    pub blocks_notifier: BlockStream,
    pub blockinfo_notifier: BlockInfoStream,
    pub slot_notifier: SlotStream,
    pub vote_account_notifier: VoteAccountStream,
    pub cluster_info_notifier: ClusterInfoStream,
}
