use std::sync::Arc;

use anyhow::{bail, Context};
use dashmap::DashMap;
use futures::StreamExt;
use jsonrpsee::SubscriptionSink;
use log::info;
use solana_client::{
    nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient},
    rpc_client::SerializableTransaction,
    rpc_config::{RpcBlockSubscribeConfig, RpcBlockSubscribeFilter},
    rpc_response::{Response as RpcResponse, RpcResponseContext},
};
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};

use solana_transaction_status::{
    TransactionConfirmationStatus, TransactionStatus, UiConfirmedBlock, UiTransactionStatusMeta,
};
use tokio::{
    sync::{mpsc::Sender, RwLock},
    task::JoinHandle,
};

use crate::postgres::{Postgres, PostgresBlock};

use super::{TxProps, TxSender};

/// Background worker which listen's to new blocks
/// and keeps a track of confirmed txs
#[derive(Clone)]
pub struct BlockListener {
    pub_sub_client: Arc<PubsubClient>,
    commitment_config: CommitmentConfig,
    tx_sender: TxSender,
    block_store: Arc<DashMap<String, BlockInformation>>,
    latest_block_hash: Arc<RwLock<String>>,
    pub signature_subscribers: Arc<DashMap<String, SubscriptionSink>>,
}

#[derive(Clone)]
pub struct BlockInformation {
    pub slot: u64,
    pub block_height: u64,
}

pub struct BlockListnerNotificatons {
    pub block: Sender<UiConfirmedBlock>,
    pub tx: Sender<TxProps>,
}

impl BlockListener {
    pub async fn new(
        pub_sub_client: Arc<PubsubClient>,
        rpc_client: Arc<RpcClient>,
        tx_sender: TxSender,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<Self> {
        let (latest_block_hash, block_height) = rpc_client
            .get_latest_blockhash_with_commitment(commitment_config)
            .await?;

        let latest_block_hash = latest_block_hash.to_string();
        let slot = rpc_client
            .get_slot_with_commitment(commitment_config)
            .await?;

        Ok(Self {
            pub_sub_client,
            tx_sender,
            latest_block_hash: Arc::new(RwLock::new(latest_block_hash.clone())),
            block_store: Arc::new({
                let map = DashMap::new();
                map.insert(latest_block_hash, BlockInformation { slot, block_height });
                map
            }),
            commitment_config,
            signature_subscribers: Default::default(),
        })
    }

    pub async fn num_of_sigs_commited(&self, sigs: &[String]) -> usize {
        let mut num_of_sigs_commited = 0;
        for sig in sigs {
            if self.tx_sender.txs_sent.contains_key(sig) {
                num_of_sigs_commited += 1;
            }
        }
        num_of_sigs_commited
    }

    pub async fn get_latest_block_info(&self) -> (String, BlockInformation) {
        let blockhash = &*self.latest_block_hash.read().await;

        (
            blockhash.to_owned(),
            self.block_store
                .get(blockhash)
                .expect("Latest Block Not in Map")
                .value()
                .to_owned(),
        )
    }

    pub async fn get_block_info(&self, blockhash: &str) -> Option<BlockInformation> {
        let Some(info) = self.block_store.get(blockhash) else {
            return None;
        };

        Some(info.value().to_owned())
    }

    pub async fn get_latest_blockhash(&self) -> String {
        self.latest_block_hash.read().await.to_owned()
    }

    pub fn signature_subscribe(&self, signature: String, sink: SubscriptionSink) {
        let _ = self.signature_subscribers.insert(signature, sink);
    }

    pub fn signature_un_subscribe(&self, signature: String) {
        self.signature_subscribers.remove(&signature);
    }

    pub fn listen(self, postgres: Option<Postgres>) -> JoinHandle<anyhow::Result<()>> {
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
                            solana_transaction_status::TransactionDetails::Full,
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

                let Some(transactions) = block.transactions else {
                    continue;
                };

                let parent_slot = block.parent_slot;

                *self.latest_block_hash.write().await = blockhash.clone();
                self.block_store
                    .insert(blockhash, BlockInformation { slot, block_height });

                if let Some(postgres) = &postgres {
                    postgres
                        .send_block(PostgresBlock {
                            slot: slot as i64,
                            leader_id: 0, //FIX:
                            parent_slot: parent_slot as i64,
                        })
                        .await
                        .unwrap();
                }

                for tx in transactions {
                    let Some(UiTransactionStatusMeta { err, status, .. }) = tx.meta else {
                        info!("tx with no meta");
                        continue;
                    };

                    let Some(tx) = tx.transaction.decode() else {
                        info!("unable to decode tx");
                        continue;
                    };

                    let sig = tx.get_signature().to_string();

                    if let Some(mut tx_status) = self.tx_sender.txs_sent.get_mut(&sig) {
                        tx_status.value_mut().status = Some(TransactionStatus {
                            slot,
                            confirmations: None,
                            status,
                            err: err.clone(),
                            confirmation_status: Some(comfirmation_status.clone()),
                        });
                    };

                    // subscribers
                    if let Some((_sig, mut sink)) = self.signature_subscribers.remove(&sig) {
                        // none if transaction succeeded
                        sink.send(&RpcResponse {
                            context: RpcResponseContext {
                                slot,
                                api_version: None,
                            },
                            value: serde_json::json!({ "err": err }),
                        })?;
                    }
                }
            }

            bail!("Stopped Listening to {commitment:?} blocks")
        })
    }
}
