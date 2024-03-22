use std::sync::Arc;

use solana_rpc_client_api::response::{RpcContactInfo, RpcVoteAccountStatus};
use tokio::sync::broadcast::Receiver;

use crate::structures::block_info::BlockInfo;
use crate::{
    structures::{produced_block::ProducedBlock, slot_notification::SlotNotification},
    traits::subscription_sink::SubscriptionSink,
};

pub type BlockStream = Receiver<ProducedBlock>;
pub type BlockInfoStream = Receiver<BlockInfo>;
pub type SlotStream = Receiver<SlotNotification>;
pub type VoteAccountStream = Receiver<RpcVoteAccountStatus>;
pub type ClusterInfoStream = Receiver<Vec<RpcContactInfo>>;
pub type SubscptionHanderSink = Arc<dyn SubscriptionSink>;
