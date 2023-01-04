use std::sync::Arc;

use anyhow::{bail, Context};
use dashmap::DashMap;
use futures::StreamExt;
use jsonrpsee::SubscriptionSink;
use log::info;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcBlockSubscribeConfig, RpcBlockSubscribeFilter};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};

use solana_sdk::transaction::TransactionError;
use solana_transaction_status::{TransactionConfirmationStatus, TransactionStatus};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Background worker which listen's to new blocks
/// and keeps a track of confirmed txs
#[derive(Clone)]
pub struct BlockListener {
    pub_sub_client: Arc<PubsubClient>,
    pub blocks: Arc<DashMap<String, TransactionStatus>>,
    commitment_config: CommitmentConfig,
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
        rpc_client: Arc<RpcClient>,
        ws_url: &str,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<Self> {
        let pub_sub_client = Arc::new(PubsubClient::new(ws_url).await?);
        let (latest_block_hash, block_height) = rpc_client
            .get_latest_blockhash_with_commitment(commitment_config)
            .await?;

        Ok(Self {
            pub_sub_client,
            blocks: Default::default(),
            latest_block_info: Arc::new(RwLock::new(BlockInformation {
                slot: rpc_client.get_slot().await?,
                blockhash: latest_block_hash.to_string(),
                block_height,
            })),
            commitment_config,
            signature_subscribers: Default::default(),
        })
    }

    /// # Return
    /// commitment_level for the list of txs from the cache
    pub async fn get_signature_statuses(&self, sigs: &[String]) -> Vec<Option<TransactionStatus>> {
        let mut commitment_levels = Vec::with_capacity(sigs.len());

        for sig in sigs {
            commitment_levels.push(self.blocks.get(sig).map(|some| some.value().clone()));
        }

        commitment_levels
    }

    pub async fn get_slot(&self) -> u64 {
        self.latest_block_info.read().await.slot
    }

    pub async fn get_latest_blockhash(&self) -> (String, u64) {
        let block = self.latest_block_info.read().await;

        (block.blockhash.clone(), block.block_height)
    }

    pub fn signature_subscribe(&self, signature: String, sink: SubscriptionSink) {
        self.signature_subscribers.insert(signature, sink).unwrap();
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
                    info!("{comfirmation_status:?} {sig}");

                    // subscribers
                    if let Some((_sig, mut sink)) = self.signature_subscribers.remove(&sig) {
                        // none if transaction succeeded
                        sink.send::<Option<TransactionError>>(&None).unwrap();
                    }

                    self.blocks.insert(
                        sig,
                        TransactionStatus {
                            slot,
                            confirmations: None, //TODO: talk about this
                            status: Ok(()),      // legacy field
                            err: None,
                            confirmation_status: Some(comfirmation_status.clone()),
                        },
                    );
                }
            }

            bail!("Stopped Listening to {commitment:?} blocks")
        })
    }
}
