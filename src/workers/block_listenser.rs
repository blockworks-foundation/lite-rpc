use std::sync::Arc;

use anyhow::{bail, Context};
use dashmap::DashMap;
use futures::StreamExt;
use jsonrpsee::SubscriptionSink;
use log::info;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client::rpc_client::SerializableTransaction;
use solana_rpc_client_api::{
    config::{RpcBlockSubscribeConfig, RpcBlockSubscribeFilter},
    response::{Response as RpcResponse, RpcResponseContext},
};

use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};

use solana_transaction_status::{
    option_serializer::OptionSerializer, RewardType, TransactionConfirmationStatus,
    TransactionStatus, UiConfirmedBlock, UiTransactionStatusMeta,
};
use tokio::{sync::mpsc::Sender, task::JoinHandle};

use crate::{
    block_store::BlockStore,
    workers::{PostgresBlock, PostgresMsg, PostgresUpdateTx},
};

use super::{PostgresMpscSend, TxProps, TxSender};

/// Background worker which listen's to new blocks
/// and keeps a track of confirmed txs
#[derive(Clone)]
pub struct BlockListener {
    pub_sub_client: Arc<PubsubClient>,
    tx_sender: TxSender,
    block_store: BlockStore,
    pub signature_subscribers: Arc<DashMap<String, SubscriptionSink>>,
}

#[derive(Clone, Debug)]
pub struct BlockInformation {
    pub slot: u64,
    pub block_height: u64,
}

pub struct BlockListnerNotificatons {
    pub block: Sender<UiConfirmedBlock>,
    pub tx: Sender<TxProps>,
}

impl BlockListener {
    pub fn new(
        pub_sub_client: Arc<PubsubClient>,
        tx_sender: TxSender,
        block_store: BlockStore,
    ) -> Self {
        Self {
            pub_sub_client,
            tx_sender,
            block_store,
            signature_subscribers: Default::default(),
        }
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

    pub fn signature_subscribe(&self, signature: String, sink: SubscriptionSink) {
        let _ = self.signature_subscribers.insert(signature, sink);
    }

    pub fn signature_un_subscribe(&self, signature: String) {
        self.signature_subscribers.remove(&signature);
    }

    pub fn listen(
        self,
        commitment_config: CommitmentConfig,
        postgres: Option<PostgresMpscSend>,
    ) -> JoinHandle<anyhow::Result<()>> {
        tokio::spawn(async move {
            let commitment = commitment_config.commitment;

            let comfirmation_status = match commitment {
                CommitmentLevel::Finalized => TransactionConfirmationStatus::Finalized,
                _ => TransactionConfirmationStatus::Confirmed,
            };

            info!("Subscribing to {commitment:?} blocks");

            let (mut recv, _) = self
                .pub_sub_client
                .block_subscribe(
                    RpcBlockSubscribeFilter::All,
                    Some(RpcBlockSubscribeConfig {
                        commitment: Some(commitment_config),
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
                let slot = block.context.slot;

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

                self.block_store
                    .add_block(
                        blockhash.clone(),
                        BlockInformation { slot, block_height },
                        commitment_config,
                    )
                    .await;

                if let Some(postgres) = &postgres {
                    let Some(rewards) = block.rewards else {
                        continue;
                    };

                    let Some(leader_reward) = rewards
                        .iter()
                        .find(|reward| Some(RewardType::Fee) == reward.reward_type) else {
                        continue;
                    };

                    let _leader_id = &leader_reward.pubkey;

                    postgres
                        .send(PostgresMsg::PostgresBlock(PostgresBlock {
                            slot: slot as i64,
                            leader_id: 0, //FIX:
                            parent_slot: parent_slot as i64,
                        }))
                        .expect("Error sending block to postgres service");
                }

                for tx in transactions {
                    let Some(UiTransactionStatusMeta { err, status, compute_units_consumed ,.. }) = tx.meta else {
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

                        //
                        // Write to postgres
                        //
                        if let Some(postgres) = &postgres {
                            let cu_consumed = match compute_units_consumed {
                                OptionSerializer::Some(cu_consumed) => Some(cu_consumed as i64),
                                _ => None,
                            };

                            postgres
                                .send(PostgresMsg::PostgresUpdateTx(
                                    PostgresUpdateTx {
                                        processed_slot: slot as i64,
                                        cu_consumed,
                                        cu_requested: None, //TODO: cu requested
                                    },
                                    sig.clone(),
                                ))
                                .unwrap();
                        }
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
