use solana_lite_rpc_core::{structures::processed_block::ProcessedBlock, AnyhowJoinHandle};
use solana_rpc_client_api::response::{RpcContactInfo, RpcVoteAccountStatus};
use solana_sdk::slot_history::Slot;
use tokio::sync::broadcast::Receiver;
pub struct EndpointStreaming {
    pub blocks_notifier: Receiver<ProcessedBlock>,
    pub slot_notifier: Receiver<Slot>,
    pub vote_account_notifier: Receiver<RpcVoteAccountStatus>,
    pub cluster_info_notifier: Receiver<Vec<RpcContactInfo>>,
    pub streaming_tasks: Vec<AnyhowJoinHandle>,
}
