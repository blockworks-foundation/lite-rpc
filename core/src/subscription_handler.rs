use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use jsonrpsee::{SubscriptionMessage, SubscriptionSink};
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    slot_history::Slot,
};
use tokio::time::Instant;

use crate::block_processor::TransactionInfo;

#[derive(Clone, Default)]
pub struct SubscriptionHandler {
    pub signature_subscribers:
        Arc<DashMap<(String, CommitmentConfig), (SubscriptionSink, Instant)>>,
}

impl SubscriptionHandler {
    #[allow(deprecated)]
    pub fn get_supported_commitment_config(
        commitment_config: CommitmentConfig,
    ) -> CommitmentConfig {
        match commitment_config.commitment {
            CommitmentLevel::Finalized | CommitmentLevel::Root | CommitmentLevel::Max => {
                CommitmentConfig {
                    commitment: CommitmentLevel::Finalized,
                }
            }
            _ => CommitmentConfig {
                commitment: CommitmentLevel::Confirmed,
            },
        }
    }

    pub fn signature_subscribe(
        &self,
        signature: String,
        commitment_config: CommitmentConfig,
        sink: SubscriptionSink,
    ) {
        let commitment_config = Self::get_supported_commitment_config(commitment_config);
        self.signature_subscribers
            .insert((signature, commitment_config), (sink, Instant::now()));
    }

    pub fn signature_un_subscribe(&self, signature: String, commitment_config: CommitmentConfig) {
        let commitment_config = Self::get_supported_commitment_config(commitment_config);
        self.signature_subscribers
            .remove(&(signature, commitment_config));
    }

    pub async fn notify(
        &self,
        slot: Slot,
        transaction_info: &TransactionInfo,
        commitment_config: CommitmentConfig,
    ) {
        if let Some((_sig, (sink, _))) = self
            .signature_subscribers
            .remove(&(transaction_info.signature.clone(), commitment_config))
        {
            // none if transaction succeeded
            let _res = sink
                .send(
                    SubscriptionMessage::from_json(&RpcResponse {
                        context: RpcResponseContext {
                            slot,
                            api_version: None,
                        },
                        value: serde_json::json!({ "err": transaction_info.err }),
                    })
                    .unwrap(),
                )
                .await;
        }
    }

    pub fn clean(&self, ttl_duration: Duration) {
        self.signature_subscribers
            .retain(|_k, (sink, instant)| !sink.is_closed() && instant.elapsed() < ttl_duration);
    }

    pub fn number_of_subscribers(&self) -> usize {
        self.signature_subscribers.len()
    }
}
