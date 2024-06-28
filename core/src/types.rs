use std::sync::Arc;

use solana_rpc_client_api::response::{RpcContactInfo, RpcVoteAccountStatus};
use tokio::sync::broadcast::Receiver;

use crate::structures::block_info::BlockInfo;
use crate::{
    structures::{produced_block::ProducedBlock, slot_notification::SlotNotification},
    traits::subscription_sink::SubscriptionSink,
};

// full blocks, commitment level: processed, confirmed, finalized
// note: there is no guarantee about the order
// note: there is no guarantee about the order wrt commitment level
// note: there is no guarantee about the order wrt block vs block meta
pub type BlockStream = Receiver<ProducedBlock>;
// block info (slot, blockhash, etc), commitment level: processed, confirmed, finalized
// note: there is no guarantee about the order wrt commitment level
pub type BlockInfoStream = Receiver<BlockInfo>;
pub type SlotStream = Receiver<SlotNotification>;

pub type VoteAccountStream = Receiver<RpcVoteAccountStatus>;
pub type ClusterInfoStream = Receiver<Vec<RpcContactInfo>>;
pub type SubscptionHanderSink = Arc<dyn SubscriptionSink>;
