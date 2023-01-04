use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context};
use dashmap::DashMap;
use futures::StreamExt;
use log::info;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcBlockSubscribeConfig, RpcBlockSubscribeFilter};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};

use solana_transaction_status::{TransactionConfirmationStatus, TransactionStatus};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Background worker which listen's to new blocks
/// and keeps a track of confirmed txs
#[derive(Clone)]
pub struct BlockListener {
    pub_sub_client: Arc<PubsubClient>,
    pub blocks: Arc<DashMap<String, TransactionStatus>>,
    slot: Arc<AtomicU64>,
    latest_block_hash: Arc<RwLock<String>>,
    block_height: Arc<AtomicU64>,
    commitment_config: CommitmentConfig,
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
            slot: Arc::new(AtomicU64::new(rpc_client.get_slot().await?)),
            pub_sub_client,
            blocks: Default::default(),
            latest_block_hash: Arc::new(RwLock::new(latest_block_hash.to_string())),
            block_height: Arc::new(AtomicU64::new(block_height)),
            commitment_config,
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

    pub fn get_slot(&self) -> u64 {
        self.slot.load(Ordering::Relaxed)
    }

    pub async fn get_latest_blockhash(&self) -> (String, u64) {
        (
            self.latest_block_hash.read().await.clone(),
            self.block_height.load(Ordering::Relaxed),
        )
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

                self.slot.store(slot, Ordering::Relaxed);
                *self.latest_block_hash.write().await = blockhash;
                self.block_height.store(block_height, Ordering::Relaxed);

                for sig in signatures {
                    info!("{comfirmation_status:?} {sig}");

                    self.blocks.insert(
                        sig,
                        TransactionStatus {
                            slot,
                            confirmations: None, //TODO: talk about this
                            status: Ok(()),         // legacy field
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
