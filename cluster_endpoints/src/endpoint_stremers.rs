use solana_lite_rpc_core::structures::{
    processed_block::ProcessedBlock, slot_notification::SlotNotification,
};
use solana_rpc_client_api::response::{RpcContactInfo, RpcVoteAccountStatus};
use tokio::sync::broadcast::Receiver;
pub struct EndpointStreaming {
    pub blocks_notifier: Receiver<ProcessedBlock>,
    pub slot_notifier: Receiver<SlotNotification>,
    pub vote_account_notifier: Receiver<RpcVoteAccountStatus>,
    pub cluster_info_notifier: Receiver<Vec<RpcContactInfo>>,
}
