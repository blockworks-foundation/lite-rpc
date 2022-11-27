use dashmap::DashMap;
use jsonrpc_core::ErrorCode;
use solana_rpc::{rpc_subscription_tracker::{SubscriptionParams, SignatureSubscriptionParams}};
use std::{str::FromStr, sync::atomic::AtomicU64};

use crate::context::{LiteRpcSubsrciptionControl};
use {
    jsonrpc_core::{Error, Result},
    jsonrpc_derive::rpc,
    solana_rpc_client_api::{
        config::*,
    },
    solana_sdk::{
        signature::Signature,
    },
    std::{
        sync::{Arc},
    },
    solana_rpc::rpc_subscription_tracker::SubscriptionId,
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
    subscription_control: Arc<LiteRpcSubsrciptionControl>,
    current_subscriptions: Arc<DashMap<SubscriptionId, (AtomicU64,SubscriptionParams)>>,
}

impl LiteRpcPubSubImpl {
    pub fn new(
        subscription_control: Arc<LiteRpcSubsrciptionControl>,
    ) -> Self {
        Self {
            current_subscriptions : Arc::new(DashMap::new()),
            subscription_control,
        }
    }

    fn subscribe(&self, params: SubscriptionParams) -> Result<SubscriptionId> {
        match self.subscription_control.subscriptions.entry(params.clone()) {
            dashmap::mapref::entry::Entry::Occupied(x) => {
                Ok(*x.get())
            },
            dashmap::mapref::entry::Entry::Vacant(x) => {
                let new_subscription_id = self.subscription_control.last_subscription_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let new_subsription_id = SubscriptionId::from(new_subscription_id);
                x.insert(new_subsription_id);
                self.current_subscriptions.insert(new_subsription_id, (AtomicU64::new(1), params));
                Ok(new_subsription_id)
            }
        }
    }

    fn unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        match self.current_subscriptions.entry(id) {
            dashmap::mapref::entry::Entry::Occupied(x) => {
                let v = x.get();
                let count = v.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                if count == 1 { // it was the last subscription
                    self.subscription_control.subscriptions.remove(&v.1);
                    x.remove();
                }
                Ok(true)
            },
            dashmap::mapref::entry::Entry::Vacant(_) => Ok(false),
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
            enable_received_notification: false,
        };
        self.subscribe(SubscriptionParams::Signature(params))
    }

    fn signature_unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        self.unsubscribe(id)
    }

    // Get notification when slot is encountered
    fn slot_subscribe(&self) -> Result<SubscriptionId>{
        Ok(SubscriptionId::from(0))
    }

    // Unsubscribe from slot notification subscription.
    fn slot_unsubscribe(&self, id: SubscriptionId) -> Result<bool> {
        Ok(true)
    }
}