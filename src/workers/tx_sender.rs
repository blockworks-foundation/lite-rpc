use std::{
    sync::Arc,
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
    workers::{PostgresMsg, PostgresTx},
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
}

pub type WireTransaction = Vec<u8>;
const NUMBER_OF_TX_SENDERS: usize = 5;

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

        let forwarded_slot = tpu_client.estimated_current_slot().await;
        let quic_response = match tpu_client.try_send_wire_transaction_batch(txs).await {
            Ok(_) => {
                // metrics
                TXS_SENT.inc_by(sigs_and_slots.len() as u64);
                1
            }
            Err(err) => {
                TXS_SENT_ERRORS.inc_by(sigs_and_slots.len() as u64);
                warn!("{err}");
                0
            }
        };

        drop(permit);

        histo_timer.observe_duration();

        info!(
            "It took {} ms to send a batch of {} transaction(s)",
            start.elapsed().as_millis(),
            sigs_and_slots.len()
        );

        if let Some(postgres) = postgres {
            let txs = sigs_and_slots
                .into_iter()
                .map(|(signature, recent_slot)| PostgresTx {
                    signature,
                    recent_slot: recent_slot as i64,
                    forwarded_slot: forwarded_slot as i64,
                    processed_slot: None,
                    cu_consumed: None,
                    cu_requested: None,
                    quic_response,
                })
                .collect();

            postgres
                .send(PostgresMsg::PostgresTx(txs))
                .expect("Error writing to postgres service");
        }
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

            loop {
                let mut sigs_and_slots = Vec::with_capacity(tx_batch_size);
                let mut txs = Vec::with_capacity(tx_batch_size);
                let mut permit = None;

                while txs.len() <= tx_batch_size {
                    match tokio::time::timeout(tx_send_interval, recv.recv()).await {
                        Ok(value) => match value {
                            Some((sig, tx, slot)) => {
                                TXS_IN_CHANNEL.dec();
                                sigs_and_slots.push((sig, slot));
                                txs.push(tx);
                            }
                            None => {
                                bail!("Channel Disconnected");
                            }
                        },
                        Err(_) => {
                            permit = semaphore.clone().try_acquire_owned().ok();
                            if permit.is_some() {
                                // we have a permit we can send collected transaction batch
                                break;
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
                            .forward_txs(sigs_and_slots, txs, postgres, permit)
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
