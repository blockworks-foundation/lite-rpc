use std::time::{Duration, Instant};

use anyhow::bail;
use chrono::Utc;
use log::{trace, warn};
use solana_sdk::slot_history::Slot;
use tokio::sync::mpsc::Receiver;

use crate::tpu_utils::tpu_service::TpuService;
use solana_lite_rpc_core::{
    data_cache::DataCache,
    notifications::{NotificationMsg, NotificationSender, TransactionNotification},
    tx_store::TxProps,
    AnyhowJoinHandle,
};

pub type WireTransaction = Vec<u8>;

#[derive(Clone, Debug)]
pub struct TransactionInfo {
    pub signature: String,
    pub slot: Slot,
    pub transaction: WireTransaction,
    pub last_valid_block_height: u64,
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
        transaction_infos: Vec<TransactionInfo>,
        notifier: Option<NotificationSender>,
    ) {
        if transaction_infos.is_empty() {
            return;
        }

        let start = Instant::now();

        let tpu_client = self.tpu_service.clone();
        let txs_sent = self.data_cache.txs.clone();

        for transaction_info in &transaction_infos {
            trace!("sending transaction {}", transaction_info.signature);
            txs_sent.insert(
                transaction_info.signature.clone(),
                TxProps {
                    status: None,
                    last_valid_blockheight: transaction_info.last_valid_block_height,
                },
            );
        }

        let forwarded_slot = self.data_cache.slot_cache.get_current_slot();
        let forwarded_local_time = Utc::now();

        let mut quic_responses = vec![];
        for transaction_info in transaction_infos.iter() {
            txs_sent.insert(
                transaction_info.signature.clone(),
                TxProps::new(transaction_info.last_valid_block_height),
            );
            let quic_response = match tpu_client.send_transaction(
                transaction_info.signature.clone(),
                transaction_info.transaction.clone(),
            ) {
                Ok(_) => 1,
                Err(err) => {
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
        trace!(
            "It took {} ms to send a batch of {} transaction(s)",
            start.elapsed().as_millis(),
            transaction_infos.len()
        );
    }

    /// retry and confirm transactions every 2ms (avg time to confirm tx)
    pub fn execute(
        self,
        mut recv: Receiver<TransactionInfo>,
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

                self.forward_txs(transaction_infos, notifier.clone()).await;
            }
        })
    }
}
