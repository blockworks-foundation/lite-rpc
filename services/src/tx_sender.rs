use std::time::{Duration, Instant};

use anyhow::bail;
use chrono::Utc;
use log::{trace, warn};

use prometheus::{
    core::GenericGauge, histogram_opts, opts, register_histogram, register_int_counter,
    register_int_gauge, Histogram, IntCounter,
};
use tokio::sync::mpsc::Receiver;

use crate::tpu_utils::tpu_service::TpuService;
use solana_lite_rpc_core::{
    stores::{data_cache::DataCache, tx_store::TxProps},
    structures::{
        notifications::{NotificationMsg, NotificationSender, TransactionNotification},
        transaction_sent_info::SentTransactionInfo,
    },
    AnyhowJoinHandle,
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

// making 250 as sleep time will effectively make lite rpc send
// (1000/250) * 5 * 512 = 10240 tps
const INTERVAL_PER_BATCH_IN_MS: u64 = 50;
const MAX_BATCH_SIZE_IN_PER_INTERVAL: usize = 2000;

/// Retry transactions to a maximum of `u16` times, keep a track of confirmed transactions
#[derive(Clone)]
pub struct TxSender {
    /// TpuClient to call the tpu port
    tpu_service: TpuService,
    data_cache: DataCache,
}

impl TxSender {
    pub fn new(data_cache: DataCache, tpu_service: TpuService) -> Self {
        Self {
            tpu_service,
            data_cache,
        }
    }

    /// retry enqued_tx(s)
    async fn forward_txs(
        &self,
        transaction_infos: Vec<SentTransactionInfo>,
        notifier: Option<NotificationSender>,
    ) {
        if transaction_infos.is_empty() {
            return;
        }

        let histo_timer = TT_SENT_TIMER.start_timer();
        let start = Instant::now();

        let tpu_client = self.tpu_service.clone();
        let txs_sent = self.data_cache.txs.clone();
        let forwarded_slot = self.data_cache.slot_cache.get_current_slot();
        let forwarded_local_time = Utc::now();

        let mut quic_responses = vec![];
        for transaction_info in transaction_infos.iter() {
            trace!("sending transaction {}", transaction_info.signature);
            txs_sent.insert(
                transaction_info.signature.clone(),
                TxProps {
                    status: None,
                    last_valid_blockheight: transaction_info.last_valid_block_height,
                    sent_by_lite_rpc: true,
                },
            );

            let quic_response = match tpu_client.send_transaction(transaction_info) {
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
            let notification_msgs = transaction_infos
                .iter()
                .enumerate()
                .map(|(index, transaction_info)| TransactionNotification {
                    signature: transaction_info.signature.clone(),
                    recent_slot: transaction_info.slot,
                    forwarded_slot,
                    forwarded_local_time,
                    processed_slot: None,
                    cu_consumed: None,
                    cu_requested: None,
                    quic_response: quic_responses[index],
                })
                .collect();
            // ignore error on sent because the channel may be already closed
            let _ = notifier.send(NotificationMsg::TxNotificationMsg(notification_msgs));
        }
        histo_timer.observe_duration();
        trace!(
            "It took {} ms to send a batch of {} transaction(s)",
            start.elapsed().as_millis(),
            transaction_infos.len()
        );
    }

    /// retry and confirm transactions every 2ms (avg time to confirm tx)
    pub fn execute(
        self,
        mut recv: Receiver<SentTransactionInfo>,
        notifier: Option<NotificationSender>,
    ) -> AnyhowJoinHandle {
        tokio::spawn(async move {
            loop {
                let mut transaction_infos = Vec::with_capacity(MAX_BATCH_SIZE_IN_PER_INTERVAL);
                let mut timeout_interval = INTERVAL_PER_BATCH_IN_MS;

                // In solana there in sig verify stage rate is limited to 2000 txs in 50ms
                // taking this as reference
                while transaction_infos.len() <= MAX_BATCH_SIZE_IN_PER_INTERVAL {
                    let instance = tokio::time::Instant::now();
                    match tokio::time::timeout(Duration::from_millis(timeout_interval), recv.recv())
                        .await
                    {
                        Ok(value) => match value {
                            Some(transaction_info) => {
                                TXS_IN_CHANNEL.dec();

                                // duplicate transaction
                                if self
                                    .data_cache
                                    .txs
                                    .contains_key(&transaction_info.signature)
                                {
                                    continue;
                                }
                                transaction_infos.push(transaction_info);
                                // update the timeout inteval
                                timeout_interval = timeout_interval
                                    .saturating_sub(instance.elapsed().as_millis() as u64)
                                    .max(1);
                            }
                            None => {
                                log::error!("Channel Disconnected");
                                bail!("Channel Disconnected");
                            }
                        },
                        Err(_) => {
                            break;
                        }
                    }
                }

                if transaction_infos.is_empty() {
                    continue;
                }

                TX_BATCH_SIZES.set(transaction_infos.len() as i64);

                self.forward_txs(transaction_infos, notifier.clone()).await;
            }
        })
    }
}
