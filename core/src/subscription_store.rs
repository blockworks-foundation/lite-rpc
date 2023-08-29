use std::{hash::Hash, sync::Arc, time::Duration};

use dashmap::DashMap;
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};
use tokio::time::Instant;

use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};

use crate::data_cache::TxSubKey;
use crate::structures::transaction_info::TransactionInfo;
use crate::subscription_sink::SubscriptionSink;

pub type Subscriber = (Box<dyn SubscriptionSink>, Instant);

#[derive(Clone, Default)]
pub struct SubscriptionStore<T: Hash + Eq> {
    pub subs: Arc<DashMap<T, Subscriber>>,
}

impl<T: Hash + Eq> SubscriptionStore<T> {
    pub fn subscribe(&self, t: T, sink: Box<dyn SubscriptionSink>) {
        self.subs.insert(t, (sink, Instant::now()));
    }

    pub fn unsubscribe(&self, t: &T) {
        self.subs.remove(t);
    }

    pub async fn notify(&self, t: &T, message: &serde_json::Value) {
        if let Some((_, (sink, _))) = self.subs.remove(t) {
            sink.send(message).await;
        }
    }

    pub fn clean(&self, ttl_duration: Duration) {
        self.subs
            .retain(|_, (sink, instant)| !sink.is_closed() && instant.elapsed() < ttl_duration);
    }

    pub fn number_of_subscribers(&self) -> usize {
        self.subs.len()
    }
}

impl SubscriptionStore<TxSubKey> {
    pub async fn notify_tx(&self, slot: Slot, tx: &TransactionInfo, commitment: CommitmentConfig) {
        self.notify(
            &(tx.signature.clone(), commitment),
            &serde_json::to_value(RpcResponse {
                context: RpcResponseContext {
                    slot,
                    api_version: None,
                },
                // none if transaction succeeded
                value: serde_json::json!({ "err": tx.err }),
            })
            .unwrap(),
        )
        .await;
    }
}
