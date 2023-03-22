use std::{
    collections::VecDeque,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use dashmap::DashMap;
use jsonrpsee::SubscriptionSink;
use log::{info, warn};
use prometheus::{
    core::GenericGauge, histogram_opts, opts, register_histogram, register_int_counter,
    register_int_gauge, Histogram, IntCounter,
};

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
use tokio::{
    sync::{mpsc::Sender, Mutex},
    task::JoinHandle,
    time::Instant,
};

use crate::{
    block_store::{BlockInformation, BlockStore},
    workers::{PostgresBlock, PostgresMsg, PostgresUpdateTx, MESSAGES_IN_POSTGRES_CHANNEL},
};

use super::{PostgresMpscSend, TxProps, TxSender};

lazy_static::lazy_static! {
    static ref TT_RECV_CON_BLOCK: Histogram = register_histogram!(histogram_opts!(
        "literpc_tt_recv_con_block",
        "Time to receive confirmed block from block subscribe",
    ))
    .unwrap();
    static ref TT_RECV_FIN_BLOCK: Histogram = register_histogram!(histogram_opts!(
        "literpc_tt_recv_fin_block",
        "Time to receive finalized block from block subscribe",
    ))
    .unwrap();
    static ref FIN_BLOCKS_RECV: IntCounter =
    register_int_counter!(opts!("literpc_fin_blocks_recv", "Number of Finalized Blocks Received")).unwrap();
    static ref CON_BLOCKS_RECV: IntCounter =
    register_int_counter!(opts!("literpc_con_blocks_recv", "Number of Confirmed Blocks Received")).unwrap();
    static ref INCOMPLETE_FIN_BLOCKS_RECV: IntCounter =
    register_int_counter!(opts!("literpc_incomplete_fin_blocks_recv", "Number of Incomplete Finalized Blocks Received")).unwrap();
    static ref INCOMPLETE_CON_BLOCKS_RECV: IntCounter =
    register_int_counter!(opts!("literpc_incomplete_con_blocks_recv", "Number of Incomplete Confirmed Blocks Received")).unwrap();
    static ref TXS_CONFIRMED: IntCounter =
    register_int_counter!(opts!("literpc_txs_confirmed", "Number of Transactions Confirmed")).unwrap();
    static ref TXS_FINALIZED: IntCounter =
    register_int_counter!(opts!("literpc_txs_finalized", "Number of Transactions Finalized")).unwrap();
    static ref ERRORS_WHILE_FETCHING_SLOTS: IntCounter =
    register_int_counter!(opts!("literpc_error_while_fetching_slots", "Number of errors while fetching slots")).unwrap();

    static ref BLOCKS_IN_CONFIRMED_QUEUE: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_blocks_in_confirmed_queue", "Number of confirmed blocks waiting to deque")).unwrap();
    static ref BLOCKS_IN_FINALIZED_QUEUE: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_blocks_in_finalized_queue", "Number of finalized blocks waiting to deque")).unwrap();

    static ref BLOCKS_IN_RETRY_QUEUE: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_blocks_in_retry_queue", "Number of blocks waiting in retry")).unwrap();
    static ref NUMBER_OF_SIGNATURE_SUBSCRIBERS: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_number_of_signature_sub", "Number of signature subscriber")).unwrap();
}

/// Background worker which listen's to new blocks
/// and keeps a track of confirmed txs
#[derive(Clone)]
pub struct BlockListener {
    tx_sender: TxSender,
    block_store: BlockStore,
    rpc_client: Arc<RpcClient>,
    signature_subscribers: Arc<DashMap<(String, CommitmentConfig), (SubscriptionSink, Instant)>>,
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
            if self.tx_sender.txs_sent_store.contains_key(sig) {
                num_of_sigs_commited += 1;
            }
        }
        num_of_sigs_commited
    }

    #[allow(deprecated)]
    fn get_supported_commitment_config(commitment_config: CommitmentConfig) -> CommitmentConfig {
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
        NUMBER_OF_SIGNATURE_SUBSCRIBERS.inc();
    }

    pub fn signature_un_subscribe(&self, signature: String, commitment_config: CommitmentConfig) {
        let commitment_config = Self::get_supported_commitment_config(commitment_config);
        self.signature_subscribers
            .remove(&(signature, commitment_config));
        NUMBER_OF_SIGNATURE_SUBSCRIBERS.dec();
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
                blockhash.clone(),
                BlockInformation {
                    slot,
                    block_height,
                    instant: Instant::now(),
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

            if let Some(mut tx_status) = self.tx_sender.txs_sent_store.get_mut(&sig) {
                //
                // Metrics
                //
                if commitment_config.is_finalized() {
                    TXS_FINALIZED.inc();
                } else {
                    TXS_CONFIRMED.inc();
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
                    MESSAGES_IN_POSTGRES_CHANNEL.inc();
                }
            };

            // subscribers
            if let Some((_sig, (mut sink, _))) =
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
                NUMBER_OF_SIGNATURE_SUBSCRIBERS.dec();
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
            MESSAGES_IN_POSTGRES_CHANNEL.inc();
        }

        Ok(())
    }
    pub fn listen(
        self,
        commitment_config: CommitmentConfig,
        postgres: Option<PostgresMpscSend>,
    ) -> JoinHandle<anyhow::Result<()>> {
        let slots_task_queue = Arc::new(Mutex::new(VecDeque::<(u64, u8)>::new()));
        let (slot_retry_queue_sx, mut slot_retry_queue_rx) = tokio::sync::mpsc::unbounded_channel();

        // task to fetch blocks
        for _i in 0..6 {
            let this = self.clone();
            let postgres = postgres.clone();
            let slots_task_queue = slots_task_queue.clone();
            let slot_retry_queue_sx = slot_retry_queue_sx.clone();

            tokio::spawn(async move {
                let slots_task_queue = slots_task_queue.clone();
                loop {
                    let (slot, error_count) = {
                        let mut queue = slots_task_queue.lock().await;
                        match queue.pop_front() {
                            Some(t) => t,
                            None => {
                                // no task
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                continue;
                            }
                        }
                    };

                    if let Err(_) = this
                        .index_slot(slot, commitment_config, postgres.clone())
                        .await
                    {
                        // usually as we index all the slots even if they are not been processed we get some errors for slot
                        // as they are not in long term storage of the rpc // we check 5 times before ignoring the slot

                        if error_count > 5 {
                            // retried for 10 times / there should be no block for this slot
                            warn!(
                                "unable to get block at slot {} and commitment {}",
                                slot, commitment_config.commitment
                            );
                            continue;
                        } else {
                            // add a task to be queued after a delay
                            let retry_at = tokio::time::Instant::now()
                                .checked_add(Duration::from_millis(100))
                                .unwrap();
                            let _ = slot_retry_queue_sx.send((slot, error_count, retry_at));
                            BLOCKS_IN_RETRY_QUEUE.inc();
                        }
                    };
                }
            });
        }

        // a task that will queue back the slots to be retried after a certain delay
        let recent_slot = Arc::new(AtomicU64::new(0));

        {
            let slots_task_queue = slots_task_queue.clone();
            let recent_slot = recent_slot.clone();
            tokio::spawn(async move {
                while let Some((slot, error_count, instant)) = slot_retry_queue_rx.recv().await {
                    BLOCKS_IN_RETRY_QUEUE.dec();
                    let recent_slot = recent_slot.load(std::sync::atomic::Ordering::Relaxed);
                    // if slot is too old ignore
                    if recent_slot.saturating_sub(slot) > 256 {
                        // slot too old to retry
                        // most probably its an empty slot
                        continue;
                    }

                    let now = tokio::time::Instant::now();
                    if now < instant {
                        tokio::time::sleep_until(instant).await;
                    }
                    let mut queue = slots_task_queue.lock().await;
                    queue.push_back((slot, error_count + 1));
                }
            });
        }

        let rpc_client = self.rpc_client.clone();
        tokio::spawn(async move {
            info!("{commitment_config:?} block listner started");

            let slots_task_queue = slots_task_queue.clone();
            let last_latest_slot = self
                .block_store
                .get_latest_block_info(commitment_config)
                .await
                .slot;
            // -5 for warmup
            let mut last_latest_slot = last_latest_slot - 5;
            recent_slot.store(last_latest_slot, std::sync::atomic::Ordering::Relaxed);

            // storage for recent slots processed
            let rpc_client = rpc_client.clone();
            loop {
                let new_slot = match rpc_client.get_slot_with_commitment(commitment_config).await {
                    Ok(new_slot) => new_slot,
                    Err(err) => {
                        warn!("Error while fetching slot {err:?}");
                        ERRORS_WHILE_FETCHING_SLOTS.inc();
                        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                        continue;
                    }
                };

                if last_latest_slot == new_slot {
                    warn!("No new slots");
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                    continue;
                }

                // filter already processed slots
                let new_block_slots: Vec<u64> = (last_latest_slot..new_slot).collect();
                // context for lock
                {
                    let mut lock = slots_task_queue.lock().await;
                    for slot in new_block_slots {
                        lock.push_back((slot, 0));
                    }

                    if commitment_config.is_finalized() {
                        BLOCKS_IN_FINALIZED_QUEUE.set(lock.len() as i64);
                    } else {
                        BLOCKS_IN_CONFIRMED_QUEUE.set(lock.len() as i64);
                    }
                }

                last_latest_slot = new_slot;
                recent_slot.store(last_latest_slot, std::sync::atomic::Ordering::Relaxed);

                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            }
        })
    }

    pub fn clean(&self, ttl_duration: Duration) {
        let length_before = self.signature_subscribers.len();
        self.signature_subscribers
            .retain(|_k, (sink, instant)| !sink.is_closed() && instant.elapsed() < ttl_duration);

        NUMBER_OF_SIGNATURE_SUBSCRIBERS.set(self.signature_subscribers.len() as i64);
        info!(
            "Cleaned {} Signature Subscribers",
            length_before - self.signature_subscribers.len()
        );
    }
}
