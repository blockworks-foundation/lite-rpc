use std::sync::Arc;
use solana_rpc_client_api::response::{RpcContactInfo, RpcVoteAccountStatus};
use tokio::sync::broadcast::Receiver;
use crate::traits::subscription_sink::SubscriptionSink;

pub type VoteAccountStream = Receiver<RpcVoteAccountStatus>;
pub type ClusterInfoStream = Receiver<Vec<RpcContactInfo>>;
pub type SubscptionHanderSink = Arc<dyn SubscriptionSink>;
