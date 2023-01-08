use std::sync::Arc;

use anyhow::{bail, Context};
use dashmap::DashMap;
use futures::StreamExt;
use jsonrpsee::SubscriptionSink;
use log::{info, warn};
use solana_client::{
    nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient},
    rpc_config::{RpcBlockSubscribeConfig, RpcBlockSubscribeFilter},
    rpc_response::{Response as RpcResponse, RpcResponseContext},
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    transaction::TransactionError,
};

use solana_transaction_status::{TransactionConfirmationStatus, TransactionStatus};
use tokio::{sync::RwLock, task::JoinHandle};

/// Background worker which listen's to new blocks
/// and keeps a track of confirmed txs
#[derive(Clone)]
pub struct BlockListener {
    pub_sub_client: Arc<PubsubClient>,
    commitment_config: CommitmentConfig,
    txs_sent: Arc<DashMap<String, Option<TransactionStatus>>>,
    latest_block_info: Arc<RwLock<BlockInformation>>,
    signature_subscribers: Arc<DashMap<String, SubscriptionSink>>,
}

struct BlockInformation {
    pub slot: u64,
    pub blockhash: String,
    pub block_height: u64,
}

impl BlockListener {
    pub async fn new(
        pub_sub_client: Arc<PubsubClient>,
        rpc_client: Arc<RpcClient>,
        txs_sent: Arc<DashMap<String, Option<TransactionStatus>>>,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<Self> {
        let (latest_block_hash, block_height) = rpc_client
            .get_latest_blockhash_with_commitment(commitment_config)
            .await?;

        Ok(Self {
            pub_sub_client,
            txs_sent,
            latest_block_info: Arc::new(RwLock::new(BlockInformation {
                slot: rpc_client.get_slot().await?,
                blockhash: latest_block_hash.to_string(),
                block_height,
            })),
            commitment_config,
            signature_subscribers: Default::default(),
        })
    }

    pub async fn num_of_sigs_commited(&self, sigs: &[String]) -> usize {
        let mut num_of_sigs_commited = 0;
        for sig in sigs {
            if self.txs_sent.contains_key(sig) {
                num_of_sigs_commited += 1;
            }
        }
        num_of_sigs_commited
    }

    pub async fn get_slot(&self) -> u64 {
        self.latest_block_info.read().await.slot
    }

    pub async fn get_latest_blockhash(&self) -> (String, u64) {
        let block = self.latest_block_info.read().await;

        (block.blockhash.clone(), block.block_height)
    }

    pub fn signature_subscribe(&self, signature: String, sink: SubscriptionSink) {
//        warn!("subscribing {signature}");

        let _ = self.signature_subscribers.insert(signature, sink);
    }

    pub fn signature_un_subscribe(&self, signature: String) {
        self.signature_subscribers.remove(&signature);
    }

    pub fn listen(self) -> JoinHandle<anyhow::Result<()>> {
        tokio::spawn(async move {
            info!("Subscribing to blocks");

            let commitment = self.commitment_config.commitment;

            let comfirmation_status = match commitment {
                CommitmentLevel::Finalized => TransactionConfirmationStatus::Finalized,
                _ => TransactionConfirmationStatus::Confirmed,
            };

            let (mut recv, _) = self
                .pub_sub_client
                .block_subscribe(
                    RpcBlockSubscribeFilter::All,
                    Some(RpcBlockSubscribeConfig {
                        commitment: Some(self.commitment_config),
                        encoding: None,
                        transaction_details: Some(
                            solana_transaction_status::TransactionDetails::Signatures,
                        ),
                        show_rewards: None,
                        max_supported_transaction_version: None,
                    }),
                )
                .await
                .context("Error calling block_subscribe")?;

            info!("Listening to {commitment:?} blocks");

            while let Some(block) = recv.as_mut().next().await {
                let slot = block.value.slot;

                let Some(block) = block.value.block else {
                    continue;
                };

                let Some(block_height) = block.block_height else {
                    continue;
                };

                let blockhash = block.blockhash;

                let Some(signatures) = block.signatures else {
                    continue;
                };

                *self.latest_block_info.write().await = BlockInformation {
                    slot,
                    blockhash,
                    block_height,
                };

                for sig in signatures {
                    if let Some(mut tx_status) = self.txs_sent.get_mut(&sig) {
                        *tx_status.value_mut() = Some(TransactionStatus {
                            slot,
                            confirmations: None, //TODO: talk about this
                            status: Ok(()),      // legacy field
                            err: None,
                            confirmation_status: Some(comfirmation_status.clone()),
                        });
                    };

                    // subscribers
                    if let Some((sig, mut sink)) = self.signature_subscribers.remove(&sig) {
//                        info!("notification {}", sig);
                        // none if transaction succeeded
                        sink.send(&RpcResponse {
                            context: RpcResponseContext {
                                slot,
                                api_version: None,
                            },
                            value: serde_json::json!({ "err": None::<TransactionError> }),
                        })
                        .unwrap();
                    }
                }
            }

            bail!("Stopped Listening to {commitment:?} blocks")
        })
    }

    pub fn capture_metrics(self) -> JoinHandle<anyhow::Result<()>> {
        tokio::spawn(async move { Ok(()) })
    }
}
