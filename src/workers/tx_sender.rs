use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::bail;
use dashmap::DashMap;
use log::{info, warn};

use prometheus::{
    core::GenericGauge, histogram_opts, opts, register_histogram, register_int_counter,
    register_int_gauge, Histogram, IntCounter,
};
use solana_transaction_status::TransactionStatus;
use tokio::{
    sync::Semaphore,
    sync::{mpsc::UnboundedReceiver, OwnedSemaphorePermit},
    task::JoinHandle,
};

use crate::{
    bridge::TXS_IN_CHANNEL,
    tpu_manager::TpuManager,
    workers::{PostgresMsg, PostgresTx, MESSAGES_IN_POSTGRES_CHANNEL},
};

use super::PostgresMpscSend;

lazy_static::lazy_static! {
    static ref TXS_SENT: IntCounter =
        register_int_counter!("literpc_txs_sent", "Number of transactions forwarded to tpu").unwrap();
    static ref TXS_SENT_ERRORS: IntCounter =
    register_int_counter!("literpc_txs_sent_errors", "Number of errors while transactions forwarded to tpu").unwrap();
    static ref TX_BATCH_SIZES: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_tx_batch_size", "batchsize of tx sent by literpc")).unwrap();
    static ref TT_SENT_TIMER: Histogram = register_histogram!(histogram_opts!(
        "literpc_txs_send_timer",
        "Time to send transaction batch",
    ))
    .unwrap();
    static ref TX_TIMED_OUT: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_tx_timeout", "Number of transactions that timeout")).unwrap();
    static ref TOKIO_SEND_TASKS: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_send_tasks_count", "Number of tasks sending confirmed transactions")).unwrap();
}

pub type WireTransaction = Vec<u8>;
const NUMBER_OF_TX_SENDERS: usize = 7;
// making 250 as sleep time will effectively make lite rpc send
// (1000/250) * 5 * 512 = 10240 tps
const SLEEP_TIME_FOR_SENDING_TASK_MS: u64 = 250;
const MAX_NUMBER_OF_TOKIO_TASKS_SENDING_TXS: u64 = 1024;

/// Retry transactions to a maximum of `u16` times, keep a track of confirmed transactions
#[derive(Clone)]
pub struct TxSender {
    /// Tx(s) forwarded to tpu
    pub txs_sent_store: Arc<DashMap<String, TxProps>>,
    /// TpuClient to call the tpu port
    pub tpu_manager: Arc<TpuManager>,
}

/// Transaction Properties
pub struct TxProps {
    pub status: Option<TransactionStatus>,
    /// Time at which transaction was forwarded
    pub sent_at: Instant,
}

impl Default for TxProps {
    fn default() -> Self {
        Self {
            status: Default::default(),
            sent_at: Instant::now(),
        }
    }
}

impl TxSender {
    pub fn new(tpu_manager: Arc<TpuManager>) -> Self {
        Self {
            tpu_manager,
            txs_sent_store: Default::default(),
        }
    }

    /// retry enqued_tx(s)
    async fn forward_txs(
        &self,
        sigs_and_slots: Vec<(String, u64)>,
        txs: Vec<WireTransaction>,
        postgres: Option<PostgresMpscSend>,
        permit: OwnedSemaphorePermit,
        tasks_counter: Arc<AtomicU64>,
    ) {
        assert_eq!(sigs_and_slots.len(), txs.len());

        if sigs_and_slots.is_empty() {
            return;
        }

        let histo_timer = TT_SENT_TIMER.start_timer();
        let start = Instant::now();

        let tpu_client = self.tpu_manager.clone();
        let txs_sent = self.txs_sent_store.clone();

        for (sig, _) in &sigs_and_slots {
            txs_sent.insert(sig.to_owned(), TxProps::default());
        }

        let forwarded_slot = tpu_client.estimated_current_slot().await as i64;
        let forwarded_local_time = chrono::offset::Utc::now();
        let transaction_batch_size = txs.len() as u64;
        let current_nb_tasks = tasks_counter.fetch_add(1, Ordering::Relaxed);
        TOKIO_SEND_TASKS.set((current_nb_tasks + 1) as i64);
        let tasks_counter_clone = tasks_counter.clone();

        tokio::spawn(async move {
            let quic_response = match tpu_client.try_send_wire_transaction_batch(txs).await {
                Ok(_) => {
                    TXS_SENT.inc_by(transaction_batch_size);
                    1
                }
                Err(err) => {
                    TXS_SENT_ERRORS.inc_by(transaction_batch_size);
                    warn!("{err}");
                    0
                }
            };
            tasks_counter.fetch_sub(1, Ordering::Relaxed);

            if let Some(postgres) = postgres {
                let postgres_msgs = sigs_and_slots
                    .iter()
                    .map(|(sig, recent_slot)| PostgresTx {
                        signature: sig.clone(),
                        recent_slot: *recent_slot as i64,
                        forwarded_slot,
                        forwarded_local_time,
                        processed_slot: None,
                        cu_consumed: None,
                        cu_requested: None,
                        quic_response,
                    })
                    .collect();

                postgres
                    .send(PostgresMsg::PostgresTx(postgres_msgs))
                    .expect("Error writing to postgres service");

                MESSAGES_IN_POSTGRES_CHANNEL.inc();
            }

            histo_timer.observe_duration();

            info!(
                "It took {} ms to send a batch of {} transaction(s)",
                start.elapsed().as_millis(),
                sigs_and_slots.len()
            );
        });
        loop {
            tokio::time::sleep(Duration::from_millis(SLEEP_TIME_FOR_SENDING_TASK_MS)).await;
            if tasks_counter_clone.load(std::sync::atomic::Ordering::Relaxed)
                < MAX_NUMBER_OF_TOKIO_TASKS_SENDING_TXS
            {
                break;
            }
            // else currently tokio has lanched too many tasks wait for some of them to get finished
        }
        drop(permit);
    }

    /// retry and confirm transactions every 2ms (avg time to confirm tx)
    pub fn execute(
        self,
        mut recv: UnboundedReceiver<(String, WireTransaction, u64)>,
        tx_batch_size: usize,
        tx_send_interval: Duration,
        postgres_send: Option<PostgresMpscSend>,
    ) -> JoinHandle<anyhow::Result<()>> {
        tokio::spawn(async move {
            info!(
                "Batching tx(s) with batch size of {tx_batch_size} every {}ms",
                tx_send_interval.as_millis()
            );

            let semaphore = Arc::new(Semaphore::new(NUMBER_OF_TX_SENDERS));

            // To limit the maximum number of background tasks sending transactions to MAX_NUMBER_OF_TOKIO_TASKS_SENDING_TXS
            let tasks_counter = Arc::new(AtomicU64::new(0));
            loop {
                let mut sigs_and_slots = Vec::with_capacity(tx_batch_size);
                let mut txs = Vec::with_capacity(tx_batch_size);
                let mut permit = None;
                let tasks_counter = tasks_counter.clone();
                let mut timeout_interval = tx_send_interval.as_millis() as u64;

                while txs.len() <= tx_batch_size {
                    let instance = tokio::time::Instant::now();
                    match tokio::time::timeout(Duration::from_millis(timeout_interval), recv.recv())
                        .await
                    {
                        Ok(value) => match value {
                            Some((sig, tx, slot)) => {
                                TXS_IN_CHANNEL.dec();
                                sigs_and_slots.push((sig, slot));
                                txs.push(tx);
                                // update the timeout inteval
                                timeout_interval = timeout_interval
                                    .saturating_sub(instance.elapsed().as_millis() as u64)
                                    .max(1);
                            }
                            None => {
                                bail!("Channel Disconnected");
                            }
                        },
                        Err(_) => {
                            permit = semaphore.clone().try_acquire_owned().ok();
                            if permit.is_some() {
                                break;
                            } else {
                                // already timed out, but could not get a permit
                                timeout_interval = 1;
                            }
                        }
                    }
                }

                assert_eq!(sigs_and_slots.len(), txs.len());

                if sigs_and_slots.is_empty() {
                    continue;
                }

                let permit = match permit {
                    Some(permit) => permit,
                    None => {
                        // get the permit
                        semaphore.clone().acquire_owned().await.unwrap()
                    }
                };

                if !txs.is_empty() {
                    TX_BATCH_SIZES.set(txs.len() as i64);
                    let postgres = postgres_send.clone();
                    let tx_sender = self.clone();
                    tokio::spawn(async move {
                        tx_sender
                            .forward_txs(
                                sigs_and_slots,
                                txs,
                                postgres,
                                permit,
                                tasks_counter.clone(),
                            )
                            .await;
                    });
                }
            }
        })
    }

    pub fn cleanup(&self, ttl_duration: Duration) {
        let length_before = self.txs_sent_store.len();
        self.txs_sent_store.retain(|_k, v| {
            let retain = v.sent_at.elapsed() < ttl_duration;
            if !retain && v.status.is_none() {
                TX_TIMED_OUT.inc();
            }
            retain
        });
        info!(
            "Cleaned {} transactions",
            length_before - self.txs_sent_store.len()
        );
    }
}
