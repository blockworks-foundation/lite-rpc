use std::time::{Duration, Instant};

use anyhow::bail;
use chrono::Utc;
use log::{info, trace, warn};

use prometheus::{
    core::GenericGauge, histogram_opts, opts, register_histogram, register_int_counter,
    register_int_gauge, Histogram, IntCounter,
};
use tokio::{sync::mpsc::Receiver, task::JoinHandle};

use crate::tpu_utils::tpu_service::TpuService;
use solana_lite_rpc_core::{
    notifications::{NotificationMsg, NotificationSender, TransactionNotification},
    tx_store::{TxProps, TxStore},
};

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
    pub static ref TXS_IN_CHANNEL: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_txs_in_channel", "Transactions in channel")).unwrap();

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
    txs_sent_store: TxStore,
    /// TpuClient to call the tpu port
    tpu_service: TpuService,
}

impl TxSender {
    pub fn new(txs_sent_store: TxStore, tpu_service: TpuService) -> Self {
        Self {
            tpu_service,
            txs_sent_store,
        }
    }

    /// retry enqued_tx(s)
    async fn forward_txs(
        &self,
        sigs_and_slots: Vec<(String, u64)>,
        txs: Vec<WireTransaction>,
        notifier: Option<NotificationSender>,
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
        for (tx, (signature, _)) in txs.iter().zip(sigs_and_slots.clone()) {
            txs_sent.insert(signature.to_owned(), TxProps::default());
            let quic_response = match tpu_client.send_transaction(signature.clone(), tx.clone()) {
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
        if let Some(notifier) = &notifier {
            let notification_msgs = sigs_and_slots
                .iter()
                .enumerate()
                .map(|(index, (sig, recent_slot))| TransactionNotification {
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
            notifier
                .send(NotificationMsg::TxNotificationMsg(notification_msgs))
                .expect("Error writing to notification service");
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
        notifier: Option<NotificationSender>,
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
                                TXS_IN_CHANNEL.dec();
                                if self.txs_sent_store.contains_key(&sig) {
                                    // duplicate transaction
                                    continue;
                                }
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
                    .forward_txs(sigs_and_slots, txs, notifier.clone())
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
