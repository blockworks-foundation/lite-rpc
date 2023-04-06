use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::bail;
use chrono::Utc;
use dashmap::DashMap;
use log::{info, trace, warn};

use prometheus::{
    core::GenericGauge, histogram_opts, opts, register_histogram, register_int_counter,
    register_int_gauge, Histogram, IntCounter,
};
use solana_transaction_status::TransactionStatus;
use tokio::{sync::mpsc::Receiver, task::JoinHandle};

use crate::{
    bridge::TXS_IN_CHANNEL,
    workers::{
        tpu_utils::tpu_service::TpuService, PostgresMsg, PostgresTx, MESSAGES_IN_POSTGRES_CHANNEL,
    },
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
// making 250 as sleep time will effectively make lite rpc send
// (1000/250) * 5 * 512 = 10240 tps
const INTERVAL_PER_BATCH_IN_MS: u64 = 50;
const MAX_BATCH_SIZE_IN_PER_INTERVAL: usize = 2000;

/// Retry transactions to a maximum of `u16` times, keep a track of confirmed transactions
#[derive(Clone)]
pub struct TxSender {
    /// Tx(s) forwarded to tpu
    pub txs_sent_store: Arc<DashMap<String, TxProps>>,
    /// TpuClient to call the tpu port
    pub tpu_service: Arc<TpuService>,
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
    pub fn new(tpu_service: Arc<TpuService>) -> Self {
        Self {
            tpu_service,
            txs_sent_store: Default::default(),
        }
    }

    /// retry enqued_tx(s)
    async fn forward_txs(
        &self,
        sigs_and_slots: Vec<(String, u64)>,
        txs: Vec<WireTransaction>,
        postgres: Option<PostgresMpscSend>,
    ) {
        assert_eq!(sigs_and_slots.len(), txs.len());

        if sigs_and_slots.is_empty() {
            return;
        }

        let histo_timer = TT_SENT_TIMER.start_timer();
        let start = Instant::now();

        let tpu_client = self.tpu_service.clone();
        let txs_sent = self.txs_sent_store.clone();

        for (sig, _) in &sigs_and_slots {
            trace!("sending transaction {}", sig);
            txs_sent.insert(sig.to_owned(), TxProps::default());
        }

        let forwarded_slot = tpu_client.get_estimated_slot();
        let forwarded_local_time = Utc::now();

        let mut quic_responses = vec![];
        for tx in txs {
            let quic_response = match tpu_client.send_transaction(tx) {
                Ok(_) => {
                    TXS_SENT.inc_by(1);
                    1
                }
                Err(err) => {
                    TXS_SENT_ERRORS.inc_by(1);
                    warn!("{err}");
                    0
                }
            };
            quic_responses.push(quic_response);
        }
        if let Some(postgres) = &postgres {
            let postgres_msgs = sigs_and_slots
                .iter()
                .enumerate()
                .map(|(index, (sig, recent_slot))| PostgresTx {
                    signature: sig.clone(),
                    recent_slot: *recent_slot as i64,
                    forwarded_slot: forwarded_slot as i64,
                    forwarded_local_time,
                    processed_slot: None,
                    cu_consumed: None,
                    cu_requested: None,
                    quic_response: quic_responses[index],
                })
                .collect();
            postgres
                .send(PostgresMsg::PostgresTx(postgres_msgs))
                .expect("Error writing to postgres service");

            MESSAGES_IN_POSTGRES_CHANNEL.inc();
        }
        histo_timer.observe_duration();
        trace!(
            "It took {} ms to send a batch of {} transaction(s)",
            start.elapsed().as_millis(),
            sigs_and_slots.len()
        );
    }

    /// retry and confirm transactions every 2ms (avg time to confirm tx)
    pub fn execute(
        self,
        mut recv: Receiver<(String, WireTransaction, u64)>,
        postgres_send: Option<PostgresMpscSend>,
    ) -> JoinHandle<anyhow::Result<()>> {
        tokio::spawn(async move {
            let tx_sender = self.clone();
            loop {
                let mut sigs_and_slots = Vec::with_capacity(MAX_BATCH_SIZE_IN_PER_INTERVAL);
                let mut txs = Vec::with_capacity(MAX_BATCH_SIZE_IN_PER_INTERVAL);
                let mut timeout_interval = INTERVAL_PER_BATCH_IN_MS;

                // In solana there in sig verify stage rate is limited to 2000 txs in 50ms
                // taking this as reference
                while txs.len() <= MAX_BATCH_SIZE_IN_PER_INTERVAL {
                    let instance = tokio::time::Instant::now();
                    match tokio::time::timeout(Duration::from_millis(timeout_interval), recv.recv())
                        .await
                    {
                        Ok(value) => match value {
                            Some((sig, tx, slot)) => {
                                if self.txs_sent_store.contains_key(&sig) {
                                    // duplicate transaction
                                    continue;
                                }
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
                            break;
                        }
                    }
                }

                assert_eq!(sigs_and_slots.len(), txs.len());

                if sigs_and_slots.is_empty() {
                    continue;
                }

                TX_BATCH_SIZES.set(txs.len() as i64);
                tx_sender
                    .forward_txs(sigs_and_slots, txs, postgres_send.clone())
                    .await;
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
