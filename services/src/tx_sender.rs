use std::time::Duration;

use anyhow::bail;
use chrono::Utc;
use itertools::Itertools;
use log::{trace, warn};

use prometheus::{core::GenericGauge, opts, register_int_counter, register_int_gauge, IntCounter};
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
    static ref TX_TIMED_OUT: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_tx_timeout", "Number of transactions that timeout")).unwrap();
    pub static ref TXS_IN_CHANNEL: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_txs_in_channel", "Transactions in channel")).unwrap();

}

const INTERVAL_PER_BATCH_IN_MS: Duration = Duration::from_millis(400);

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
    async fn forward_txs(&self, transaction_info: &SentTransactionInfo) {
        trace!("sending transaction {}", transaction_info.signature);
        self.data_cache.txs.insert(
            transaction_info.signature.clone(),
            TxProps {
                status: None,
                last_valid_blockheight: transaction_info.last_valid_block_height,
                sent_by_lite_rpc: true,
            },
        );

        match self.tpu_service.send_transaction(transaction_info) {
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
    }

    /// retry and confirm transactions every 2ms (avg time to confirm tx)
    pub fn execute(
        self,
        mut recv: Receiver<SentTransactionInfo>,
        notifier: Option<NotificationSender>,
    ) -> AnyhowJoinHandle {
        tokio::spawn(async move {
            let mut notifications = vec![];
            let mut interval = tokio::time::interval(INTERVAL_PER_BATCH_IN_MS);
            let notify_transaction_messages = |notifications: &mut Vec<TransactionNotification>| {
                if notifications.is_empty() {
                    // no notifications to send
                    return;
                }
                if let Some(notifier) = &notifier {
                    // send notification for sent transactions
                    let _ = notifier.send(NotificationMsg::TxNotificationMsg(
                        notifications.drain(..).collect_vec(),
                    ));
                }
            };

            loop {
                tokio::select! {
                    transaction_info = recv.recv() => {
                        if let Some(transaction_info) = transaction_info {
                            self.forward_txs(&transaction_info).await;

                            if notifier.is_some() {
                                let forwarded_slot = self.data_cache.slot_cache.get_current_slot();
                                let forwarded_local_time = Utc::now();
                                let tx_notification = TransactionNotification {
                                    signature: transaction_info.signature,
                                    recent_slot: transaction_info.slot,
                                    forwarded_slot,
                                    forwarded_local_time,
                                    processed_slot: None,
                                    cu_consumed: None,
                                    cu_requested: None,
                                    quic_response: 0,
                                };
                                notifications.push(tx_notification);
                            }
                        } else {
                            notify_transaction_messages(&mut notifications);
                            log::warn!("TxSender reciever broken");
                            break;
                        }

                    },
                    _ = interval.tick() => {
                        notify_transaction_messages(&mut notifications);
                    }
                }
            }
            bail!("Tx sender service stopped");
        })
    }
}
