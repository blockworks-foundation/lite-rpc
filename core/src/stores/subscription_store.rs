use crate::commitment_utils::Commitment;
use crate::{structures::produced_block::TransactionInfo, types::SubscptionHanderSink};
use dashmap::DashMap;
use solana_client::rpc_response::{ProcessedSignatureResult, RpcSignatureResult};
use solana_sdk::signature::Signature;
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

#[derive(Clone, Default)]
pub struct SubscriptionStore {
    pub signature_subscribers:
        Arc<DashMap<(Signature, Commitment), (SubscptionHanderSink, Instant)>>,
}

impl SubscriptionStore {
    pub fn signature_subscribe(
        &self,
        signature: Signature,
        commitment_config: CommitmentConfig,
        sink: SubscptionHanderSink,
    ) {
        self.signature_subscribers.insert(
            (signature, Commitment::from(commitment_config)),
            (sink, Instant::now()),
        );
    }

    pub fn signature_un_subscribe(
        &self,
        signature: Signature,
        commitment_config: CommitmentConfig,
    ) {
        self.signature_subscribers
            .remove(&(signature, Commitment::from(commitment_config)));
    }

    pub async fn notify(
        &self,
        slot: Slot,
        transaction_info: &TransactionInfo,
        commitment_config: CommitmentConfig,
    ) {
        if let Some((_sig, (sink, _))) = self.signature_subscribers.remove(&(
            transaction_info.signature,
            Commitment::from(commitment_config),
        )) {
            let signature_result =
                RpcSignatureResult::ProcessedSignature(ProcessedSignatureResult {
                    err: transaction_info.err.clone(),
                });
            // none if transaction succeeded
            sink.send(
                slot,
                serde_json::to_value(signature_result).expect("Should be serializable in json"),
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
