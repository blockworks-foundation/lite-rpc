use dashmap::DashMap;
use jsonrpc_core::ErrorCode;
use solana_client::{
    pubsub_client::{BlockSubscription, PubsubClientError, SignatureSubscription},
    tpu_client::TpuClientConfig,
};
use solana_pubsub_client::pubsub_client::{PubsubBlockClientSubscription, PubsubClient};
use solana_rpc::{rpc_pubsub_service::PubSubConfig, rpc_subscription_tracker::{SubscriptionControl, SubscriptionToken, SubscriptionParams, SignatureSubscriptionParams}};
use std::{thread::{Builder, JoinHandle}, sync::Mutex, str::FromStr};

use crate::context::{BlockInformation, LiteRpcContext, self};
use {
    bincode::config::Options,
    crossbeam_channel::Receiver,
    jsonrpc_core::{Error, Metadata, Result},
    jsonrpc_derive::rpc,
    solana_client::{rpc_client::RpcClient, tpu_client::TpuClient},
    solana_perf::packet::PACKET_DATA_SIZE,
    solana_rpc_client_api::{
        config::*,
        response::{Response as RpcResponse, *},
    },
    solana_sdk::{
        commitment_config::{CommitmentConfig, CommitmentLevel},
        signature::Signature,
        transaction::VersionedTransaction,
    },
    solana_client::connection_cache::ConnectionCache,
    solana_transaction_status::{TransactionBinaryEncoding, UiTransactionEncoding},
    std::{
        any::type_name,
        collections::HashMap,
        sync::{atomic::Ordering, Arc, RwLock},
    },
    jsonrpc_pubsub::{
        typed::Subscriber,
    },
    solana_rpc::rpc_subscription_tracker::SubscriptionId,
    jsonrpc_pubsub::SubscriptionId as PubSubSubscriptionId,
};

#[rpc]
pub trait LiteRpcPubSub {

    // Get notification when signature is verified
    // Accepts signature parameter as base-58 encoded string
    #[rpc(name = "signatureSubscribe")]
    fn signature_subscribe(
        &self,
        signature_str: String,
        config: Option<RpcSignatureSubscribeConfig>,
    ) -> Result<SubscriptionId>;

    // Unsubscribe from signature notification subscription.
    #[rpc(name = "signatureUnsubscribe")]
    fn signature_unsubscribe(&self, id: SubscriptionId) -> Result<bool>;

    // Get notification when slot is encountered
    #[rpc(name = "slotSubscribe")]
    fn slot_subscribe(&self) -> Result<SubscriptionId>;

    // Unsubscribe from slot notification subscription.
    #[rpc(name = "slotUnsubscribe")]
    fn slot_unsubscribe(&self, id: SubscriptionId) -> Result<bool>;

}


pub struct LiteRpcPubSubImpl {
    config: PubSubConfig,
    subscription_control: SubscriptionControl,
    current_subscriptions: Arc<DashMap<SubscriptionId, SubscriptionToken>>,
    context: Arc<LiteRpcContext>,
}

impl LiteRpcPubSubImpl {
    pub fn new(
        config : PubSubConfig,
        subscription_control : SubscriptionControl,
        context : Arc<LiteRpcContext>
    ) -> Self {
        Self {
            config,
            current_subscriptions : Arc::new(DashMap::new()),
            subscription_control,
            context
        }
    }

    fn subscribe(&self, params: SubscriptionParams) -> Result<SubscriptionId> {
        let token = self
            .subscription_control
            .subscribe(params)
            .map_err(|_| Error {
                code: ErrorCode::InternalError,
                message: "Internal Error: Subscription refused. Node subscription limit reached"
                    .into(),
                data: None,
            })?;
        let id = token.id();
        self.current_subscriptions.insert(id, token);
        Ok(id)
    }

    fn unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        if self.current_subscriptions.remove(&id).is_some() {
            Ok(true)
        } else {
            Err(Error {
                code: ErrorCode::InvalidParams,
                message: "Invalid subscription id.".into(),
                data: None,
            })
        }
    }

}

fn param<T: FromStr>(param_str: &str, thing: &str) -> Result<T> {
    param_str.parse::<T>().map_err(|_e| Error {
        code: ErrorCode::InvalidParams,
        message: format!("Invalid Request: Invalid {} provided", thing),
        data: None,
    })
}

impl LiteRpcPubSub for LiteRpcPubSubImpl {
    fn signature_subscribe(
        &self,
        signature_str: String,
        config: Option<RpcSignatureSubscribeConfig>,
    ) -> Result<SubscriptionId> {
        let config = config.unwrap_or_default();
        let params = SignatureSubscriptionParams {
            signature: param::<Signature>(&signature_str, "signature")?,
            commitment: config.commitment.unwrap_or_default(),
            enable_received_notification: config.enable_received_notification.unwrap_or_default(),
        };
        self.subscribe(SubscriptionParams::Signature(params))
    }

    fn signature_unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        self.unsubscribe(id)
    }

    // Get notification when slot is encountered
    fn slot_subscribe(&self) -> Result<SubscriptionId>{
        self.subscribe(SubscriptionParams::Slot)
    }

    // Unsubscribe from slot notification subscription.
    fn slot_unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        self.unsubscribe(id)
    }


}