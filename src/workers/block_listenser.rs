use std::{sync::Arc, time::Instant};

use dashmap::DashMap;
use futures::future::join_all;
use jsonrpsee::SubscriptionSink;
use log::{info, warn};
use prometheus::{histogram_opts, opts, register_counter, register_histogram, Counter, Histogram};

use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::{
    config::RpcBlockConfig,
    response::{Response as RpcResponse, RpcResponseContext},
};

use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    slot_history::Slot,
};

use solana_transaction_status::{
    option_serializer::OptionSerializer, RewardType, TransactionConfirmationStatus,
    TransactionDetails, TransactionStatus, UiConfirmedBlock, UiTransactionEncoding,
    UiTransactionStatusMeta,
};
use tokio::{sync::mpsc::Sender, task::JoinHandle};

use crate::{
    block_store::BlockStore,
    workers::{PostgresBlock, PostgresMsg, PostgresUpdateTx},
};

use super::{PostgresMpscSend, TxProps, TxSender};

lazy_static::lazy_static! {
    static ref TT_RECV_CON_BLOCK: Histogram = register_histogram!(histogram_opts!(
        "tt_recv_con_block",
        "Time to receive confirmed block from block subscribe",
    ))
    .unwrap();
    static ref TT_RECV_FIN_BLOCK: Histogram = register_histogram!(histogram_opts!(
        "tt_recv_fin_block",
        "Time to receive finalized block from block subscribe",
    ))
    .unwrap();
    static ref FIN_BLOCKS_RECV: Counter =
        register_counter!(opts!("fin_blocks_recv", "Number of Finalized Blocks Received")).unwrap();
    static ref CON_BLOCKS_RECV: Counter =
        register_counter!(opts!("con_blocks_recv", "Number of Confirmed Blocks Received")).unwrap();
    static ref INCOMPLETE_FIN_BLOCKS_RECV: Counter =
        register_counter!(opts!("incomplete_fin_blocks_recv", "Number of Incomplete Finalized Blocks Received")).unwrap();
    static ref INCOMPLETE_CON_BLOCKS_RECV: Counter =
        register_counter!(opts!("incomplete_con_blocks_recv", "Number of Incomplete Confirmed Blocks Received")).unwrap();
    static ref TXS_CONFIRMED: Counter =
        register_counter!(opts!("txs_confirmed", "Number of Transactions Confirmed")).unwrap();
    static ref TXS_FINALIZED: Counter =
        register_counter!(opts!("txs_finalized", "Number of Transactions Finalized")).unwrap();
}

/// Background worker which listen's to new blocks
/// and keeps a track of confirmed txs
#[derive(Clone)]
pub struct BlockListener {
    tx_sender: TxSender,
    block_store: BlockStore,
    rpc_client: Arc<RpcClient>,
    pub signature_subscribers: Arc<DashMap<(String, CommitmentConfig), SubscriptionSink>>,
}

#[derive(Clone, Debug)]
pub struct BlockInformation {
    pub slot: u64,
    pub block_height: u64,
    pub blockhash: String,
}

pub struct BlockListnerNotificatons {
    pub block: Sender<UiConfirmedBlock>,
    pub tx: Sender<TxProps>,
}

impl BlockListener {
    pub fn new(rpc_client: Arc<RpcClient>, tx_sender: TxSender, block_store: BlockStore) -> Self {
        Self {
            rpc_client,
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

    pub fn signature_subscribe(
        &self,
        signature: String,
        commitment_config: CommitmentConfig,
        sink: SubscriptionSink,
    ) {
        self.signature_subscribers
            .insert((signature, commitment_config), sink);
    }

    pub fn signature_un_subscribe(&self, signature: String, commitment_config: CommitmentConfig) {
        self.signature_subscribers
            .remove(&(signature, commitment_config));
    }

    fn increment_invalid_block_metric(commitment_config: CommitmentConfig) {
        if commitment_config.is_finalized() {
            INCOMPLETE_FIN_BLOCKS_RECV.inc();
        } else {
            INCOMPLETE_CON_BLOCKS_RECV.inc();
        }
    }

    pub async fn index_slot(
        &self,
        slot: Slot,
        commitment_config: CommitmentConfig,
        postgres: Option<PostgresMpscSend>,
    ) -> anyhow::Result<()> {
        //info!("indexing slot {} commitment {}", slot, commitment_config.commitment);
        let comfirmation_status = match commitment_config.commitment {
            CommitmentLevel::Finalized => TransactionConfirmationStatus::Finalized,
            _ => TransactionConfirmationStatus::Confirmed,
        };

        let timer = if commitment_config.is_finalized() {
            TT_RECV_FIN_BLOCK.start_timer()
        } else {
            TT_RECV_CON_BLOCK.start_timer()
        };

        let start = Instant::now();

        let block = self
            .rpc_client
            .get_block_with_config(
                slot,
                RpcBlockConfig {
                    transaction_details: Some(TransactionDetails::Full),
                    commitment: Some(commitment_config),
                    max_supported_transaction_version: Some(0),
                    encoding: Some(UiTransactionEncoding::Base64),
                    rewards: Some(true),
                },
            )
            .await?;

        timer.observe_duration();

        if commitment_config.is_finalized() {
            info!("finalized slot {}", slot);
            FIN_BLOCKS_RECV.inc();
        } else {
            CON_BLOCKS_RECV.inc();
        };

        let Some(block_height) = block.block_height else {
            Self::increment_invalid_block_metric(commitment_config);
            return Ok(());
        };

        let Some(transactions) = block.transactions else {
                Self::increment_invalid_block_metric(commitment_config);
                return Ok(());
         };

        let blockhash = block.blockhash;
        let parent_slot = block.parent_slot;

        self.block_store
            .add_block(
                BlockInformation {
                    slot,
                    block_height,
                    blockhash: blockhash.clone(),
                },
                commitment_config,
            )
            .await;

        let mut transactions_processed = 0;
        for tx in transactions {
            let Some(UiTransactionStatusMeta { err, status, compute_units_consumed ,.. }) = tx.meta else {
                info!("tx with no meta");
                continue;
            };

            let tx = match tx.transaction.decode() {
                Some(tx) => tx,
                None => {
                    warn!("transaction could not be decoded");
                    continue;
                }
            };
            transactions_processed += 1;
            let sig = tx.signatures[0].to_string();

            if let Some(mut tx_status) = self.tx_sender.txs_sent.get_mut(&sig) {
                //
                // Metrics
                //
                if status.is_ok() {
                    if commitment_config.is_finalized() {
                        TXS_FINALIZED.inc();
                    } else {
                        TXS_CONFIRMED.inc();
                    }
                }

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
            if let Some((_sig, mut sink)) =
                self.signature_subscribers.remove(&(sig, commitment_config))
            {
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

        info!(
            "Number of transactions processed {} for slot {} for commitment {} time taken {} ms",
            transactions_processed,
            slot,
            commitment_config.commitment,
            start.elapsed().as_millis()
        );

        if let Some(postgres) = &postgres {
            let Some(rewards) = block.rewards else {
                return Ok(());
            };

            let Some(leader_reward) = rewards
                      .iter()
                      .find(|reward| Some(RewardType::Fee) == reward.reward_type) else {
                return Ok(());
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

        Ok(())
    }

    pub fn listen(
        self,
        commitment_config: CommitmentConfig,
        postgres: Option<PostgresMpscSend>,
    ) -> JoinHandle<anyhow::Result<()>> {
        tokio::spawn(async move {
            let mut latest_slot = self
                .rpc_client
                .get_slot_with_commitment(commitment_config)
                .await?;

            info!("Listening to blocks {commitment_config:?}");

            let mut slot_que = Vec::new();

            loop {
                let mut new_block_slots = self
                    .rpc_client
                    .get_blocks_with_commitment(latest_slot, None, commitment_config)
                    .await?;

                if new_block_slots.is_empty() {
                    warn!("{latest_slot} No slots");
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    continue;
                }

                let new_latest_slot = *new_block_slots.last().unwrap();

                if latest_slot == new_latest_slot {
                    warn!("No new slots for {latest_slot}");
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    continue;
                }

                latest_slot = new_latest_slot;

                // reverse to put latest_slot first
                new_block_slots.reverse();

                // need not queue up new slots if they are less than 16 and there the queue is empty
                let slots_to_get_blocks = if slot_que.is_empty() && new_block_slots.len() <= 16 {
                    new_block_slots
                } else {
                    slot_que.append(&mut new_block_slots);
                    slot_que.split_off(slot_que.len().min(16))
                };

                let index_futs = slots_to_get_blocks
                    .into_iter()
                    .map(|slot| self.index_slot(slot, commitment_config, postgres.clone()));

                join_all(index_futs).await;
            }
        })
    }
}
