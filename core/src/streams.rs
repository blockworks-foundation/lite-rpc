use solana_rpc_client_api::response::{RpcContactInfo, RpcVoteAccountStatus};
use tokio::sync::broadcast::Receiver;

use crate::structures::{processed_block::ProcessedBlock, slot_notification::SlotNotification};

pub type BlockStream = Receiver<ProcessedBlock>;
pub type SlotStream = Receiver<SlotNotification>;
pub type VoteAccountStream = Receiver<RpcVoteAccountStatus>;
pub type ClusterInfoStream = Receiver<Vec<RpcContactInfo>>;
