use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{bail, Context};
use chrono::{TimeZone, Utc};
use log::{error, info, trace};
use prometheus::{
    core::GenericGauge, histogram_opts, opts, register_histogram, register_int_counter,
    register_int_gauge, Histogram, IntCounter,
};

use solana_rpc_client::nonblocking::rpc_client::RpcClient;

use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    slot_history::Slot,
};

use solana_transaction_status::{
    TransactionConfirmationStatus, TransactionStatus, UiConfirmedBlock,
};
use tokio::{sync::mpsc::Sender, task::JoinHandle, time::Instant};

use solana_lite_rpc_core::{
    block_processor::BlockProcessor,
    block_store::BlockStore,
    notifications::{
        BlockNotification, NotificationMsg, NotificationSender, TransactionUpdateNotification,
    },
    subscription_handler::{SubscptionHanderSink, SubscriptionHandler},
    tx_store::{TxProps, TxStore},
};

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
    tx_store: TxStore,
    rpc_client: Arc<RpcClient>,
    block_processor: BlockProcessor,
    subscription_handler: SubscriptionHandler,
    block_store: BlockStore,
}

pub struct BlockListnerNotificatons {
    pub block: Sender<UiConfirmedBlock>,
    pub tx: Sender<TxProps>,
}

impl BlockListener {
    pub fn new(rpc_client: Arc<RpcClient>, tx_store: TxStore, block_store: BlockStore) -> Self {
        Self {
            block_processor: BlockProcessor::new(rpc_client.clone(), Some(block_store.clone())),
            rpc_client,
            tx_store,
            subscription_handler: SubscriptionHandler::default(),
            block_store,
        }
    }

    pub async fn num_of_sigs_commited(&self, sigs: &[String]) -> usize {
        let mut num_of_sigs_commited = 0;
        for sig in sigs {
            if self.tx_store.contains_key(sig) {
                num_of_sigs_commited += 1;
            }
        }
        num_of_sigs_commited
    }

    pub fn signature_subscribe(
        &self,
        signature: String,
        commitment_config: CommitmentConfig,
        sink: SubscptionHanderSink,
    ) {
        self.subscription_handler
            .signature_subscribe(signature, commitment_config, sink);
        NUMBER_OF_SIGNATURE_SUBSCRIBERS.inc();
    }

    pub fn signature_un_subscribe(&self, signature: String, commitment_config: CommitmentConfig) {
        self.subscription_handler
            .signature_un_subscribe(signature, commitment_config);
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
        postgres: Option<NotificationSender>,
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
        let block_processor_result = self
            .block_processor
            .process(slot, commitment_config)
            .await?;

        if block_processor_result.invalid_block {
            Self::increment_invalid_block_metric(commitment_config);
            bail!("Invalid block");
        }

        timer.observe_duration();
        let mut transactions_processed = 0;
        let mut transactions_to_update = vec![];

        for tx_info in block_processor_result.transaction_infos {
            transactions_processed += 1;

            if let Some(mut tx_status) = self.tx_store.get_mut(&tx_info.signature) {
                //
                // Metrics
                //
                if commitment_config.is_finalized() {
                    TXS_FINALIZED.inc();
                } else {
                    TXS_CONFIRMED.inc();
                }

                trace!(
                    "got transaction {} confrimation level {}",
                    tx_info.signature,
                    commitment_config.commitment
                );

                tx_status.value_mut().status = Some(TransactionStatus {
                    slot,
                    confirmations: None,
                    status: tx_info.status.clone(),
                    err: tx_info.err.clone(),
                    confirmation_status: Some(comfirmation_status.clone()),
                });

                // prepare writing to postgres
                if let Some(_postgres) = &postgres {
                    transactions_to_update.push(TransactionUpdateNotification {
                        signature: tx_info.signature.clone(),
                        processed_slot: slot as i64,
                        cu_consumed: tx_info.cu_consumed,
                        cu_requested: tx_info.cu_requested,
                        cu_price: tx_info.prioritization_fees,
                    });
                }
            };

            // subscribers
            self.subscription_handler
                .notify(slot, &tx_info, commitment_config)
                .await;
        }
        //
        // Notify
        //
        if let Some(postgres) = &postgres {
            postgres
                .send(NotificationMsg::UpdateTransactionMsg(
                    transactions_to_update,
                ))
                .unwrap();
        }

        trace!(
            "Number of transactions processed {} for slot {} for commitment {} time taken {} ms",
            transactions_processed,
            slot,
            commitment_config.commitment,
            start.elapsed().as_millis()
        );

        if let Some(postgres) = &postgres {
            // TODO insert if not exists leader_id into accountaddrs

            // fetch cluster time from rpc
            let block_time = self.rpc_client.get_block_time(slot).await?;

            // fetch local time from blockstore
            let block_info = self
                .block_store
                .get_block_info(&block_processor_result.blockhash);

            postgres
                .send(NotificationMsg::BlockNotificationMsg(BlockNotification {
                    slot: slot as i64,
                    leader_id: 0, // TODO: lookup leader
                    parent_slot: block_processor_result.parent_slot as i64,
                    cluster_time: Utc.timestamp_millis_opt(block_time * 1000).unwrap(),
                    local_time: block_info.and_then(|b| b.processed_local_time),
                }))
                .expect("Error sending block to postgres service");
        }

        Ok(())
    }

    pub async fn listen(
        self,
        commitment_config: CommitmentConfig,
        notifier: Option<NotificationSender>,
        estimated_slot: Arc<AtomicU64>,
        exit_signal: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        let (slot_retry_queue_sx, mut slot_retry_queue_rx) = tokio::sync::mpsc::unbounded_channel();
        let (block_schedule_queue_sx, block_schedule_queue_rx) = async_channel::unbounded::<Slot>();

        // task to fetch blocks
        //
        let this = self.clone();
        let exit_signal_l = exit_signal.clone();
        let slot_indexer_tasks = (0..8).map(move |_| {
            let this = this.clone();
            let notifier = notifier.clone();
            let slot_retry_queue_sx = slot_retry_queue_sx.clone();
            let block_schedule_queue_rx = block_schedule_queue_rx.clone();
            let exit_signal_l = exit_signal_l.clone();
            let task: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
                loop {
                    if exit_signal_l.load(Ordering::Relaxed) {
                        break;
                    }
                    match block_schedule_queue_rx.recv().await {
                        Ok(slot) => {
                            if commitment_config.is_finalized() {
                                BLOCKS_IN_FINALIZED_QUEUE.dec();
                            } else {
                                BLOCKS_IN_CONFIRMED_QUEUE.dec();
                            }

                            if this
                                .index_slot(slot, commitment_config, notifier.clone())
                                .await
                                .is_err()
                            {
                                // add a task to be queued after a delay
                                let retry_at = tokio::time::Instant::now()
                                    .checked_add(Duration::from_millis(10))
                                    .unwrap();

                                slot_retry_queue_sx
                                    .send((slot, retry_at))
                                    .context("Error sending slot to retry queue from slot indexer task")?;

                                BLOCKS_IN_RETRY_QUEUE.inc();
                            };
                        },
                        Err(_) => {
                            // We get error because channel is empty we retry recv again
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    }
                }
                bail!("Block Slot channel closed")
            });

            task
        });

        // a task that will queue back the slots to be retried after a certain delay
        let recent_slot = Arc::new(AtomicU64::new(0));

        let slot_retry_task: JoinHandle<anyhow::Result<()>> = {
            let block_schedule_queue_sx = block_schedule_queue_sx.clone();
            let recent_slot = recent_slot.clone();
            let exit_signal_l = exit_signal.clone();
            tokio::spawn(async move {
                while let Some((slot, instant)) = slot_retry_queue_rx.recv().await {
                    if exit_signal_l.load(Ordering::Relaxed) {
                        break;
                    }

                    BLOCKS_IN_RETRY_QUEUE.dec();
                    let recent_slot = recent_slot.load(std::sync::atomic::Ordering::Relaxed);
                    // if slot is too old ignore
                    if recent_slot.saturating_sub(slot) > 128 {
                        // slot too old to retry
                        // most probably its an empty slot
                        continue;
                    }

                    if tokio::time::Instant::now() < instant {
                        tokio::time::sleep_until(instant).await;
                    }

                    if let Err(e) = block_schedule_queue_sx.send(slot).await {
                        error!("Error sending slot on {commitment_config:?} queue for block listner {e:?}");
                        continue;
                    }

                    if commitment_config.is_finalized() {
                        BLOCKS_IN_FINALIZED_QUEUE.inc();
                    } else {
                        BLOCKS_IN_CONFIRMED_QUEUE.inc();
                    }
                }

                bail!("Slot retry task exit")
            })
        };

        let get_slot_task: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            info!("{commitment_config:?} block listner started");

            let last_latest_slot = self
                .block_store
                .get_latest_block_info(commitment_config)
                .await
                .slot;
            // -5 for warmup
            let mut last_latest_slot = last_latest_slot.saturating_sub(5);
            recent_slot.store(last_latest_slot, std::sync::atomic::Ordering::Relaxed);

            loop {
                let new_slot = estimated_slot.load(Ordering::Relaxed);

                if last_latest_slot == new_slot {
                    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                    continue;
                }

                if exit_signal.load(Ordering::Relaxed) {
                    break;
                }
                // filter already processed slots
                let new_block_slots: Vec<u64> = (last_latest_slot..new_slot).collect();
                // context for lock
                {
                    for slot in new_block_slots {
                        if let Err(e) = block_schedule_queue_sx.send(slot).await {
                            error!("Error sending slot on {commitment_config:?} queue for block listner {e:?}");
                            continue;
                        }

                        if commitment_config.is_finalized() {
                            BLOCKS_IN_FINALIZED_QUEUE.inc();
                        } else {
                            BLOCKS_IN_CONFIRMED_QUEUE.inc();
                        }
                    }
                }

                last_latest_slot = new_slot;
                recent_slot.store(last_latest_slot, std::sync::atomic::Ordering::Relaxed);
            }
            Ok(())
        });

        tokio::select! {
            res = get_slot_task => {
                anyhow::bail!("Get slot task exited unexpectedly {res:?}")
            }
            res = slot_retry_task => {
                anyhow::bail!("Slot retry task exited unexpectedly {res:?}")
            },
            res = futures::future::try_join_all(slot_indexer_tasks) => {
                anyhow::bail!("Slot indexer exited unexpectedly {res:?}")
            },
        }
    }

    // continuosly poll processed blocks and feed into blockstore
    pub fn listen_processed(self, exit_signal: Arc<AtomicBool>) -> JoinHandle<anyhow::Result<()>> {
        let block_processor = self.block_processor;

        tokio::spawn(async move {
            info!("processed block listner started");

            loop {
                if exit_signal.load(Ordering::Relaxed) {
                    break;
                }

                if let Err(err) = block_processor
                    .poll_latest_block(CommitmentConfig::processed())
                    .await
                {
                    error!("Error fetching latest processed block {err:?}");
                }

                // sleep
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
            Ok(())
        })
    }

    pub fn clean(&self, ttl_duration: Duration) {
        let length_before = self.subscription_handler.number_of_subscribers();
        self.subscription_handler.clean(ttl_duration);
        NUMBER_OF_SIGNATURE_SUBSCRIBERS
            .set(self.subscription_handler.number_of_subscribers() as i64);
        info!(
            "Cleaned {} Signature Subscribers",
            length_before - self.subscription_handler.number_of_subscribers()
        );
    }
}
