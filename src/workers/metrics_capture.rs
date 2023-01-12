use std::sync::{atomic::Ordering, Arc};

use log::{info, warn};
use solana_transaction_status::TransactionConfirmationStatus;
use tokio::{sync::RwLock, task::JoinHandle};

use super::TxSender;
use serde::{Deserialize, Serialize};

/// Background worker which captures metrics
#[derive(Clone)]
pub struct MetricsCapture {
    tx_sender: TxSender,
    metrics: Arc<RwLock<Metrics>>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Metrics {
    pub txs_sent: u64,
    pub txs_confirmed: usize,
    pub txs_finalized: usize,
    pub txs_ps: u64,
    pub txs_confirmed_ps: usize,
    pub txs_finalized_ps: usize,
    pub mem_used: Option<usize>,
}

impl MetricsCapture {
    pub fn new(tx_sender: TxSender) -> Self {
        Self {
            tx_sender,
            metrics: Default::default(),
        }
    }

    pub async fn get_metrics(&self) -> Metrics {
        self.metrics.read().await.to_owned()
    }

    pub fn capture(self) -> JoinHandle<anyhow::Result<()>> {
        let mut one_second = tokio::time::interval(std::time::Duration::from_secs(1));

        tokio::spawn(async move {
            info!("Capturing Metrics");

            loop {
                one_second.tick().await;

                let txs_sent = self.tx_sender.nb_tx_sent.load(Ordering::Relaxed);
                let mut txs_confirmed: usize = 0;
                let mut txs_finalized: usize = 0;

                for tx in self.tx_sender.txs_sent.iter() {
                    if let Some(tx) = &tx.value().status {
                        match tx.confirmation_status() {
                            TransactionConfirmationStatus::Confirmed => txs_confirmed += 1,
                            TransactionConfirmationStatus::Finalized => {
                                txs_confirmed += 1;
                                txs_finalized += 1;
                            }
                            _ => (),
                        }
                    }
                }

                let mut metrics = self.metrics.write().await;

                metrics.txs_ps = txs_sent.checked_sub(metrics.txs_sent).unwrap_or_default();
                metrics.txs_confirmed_ps = txs_confirmed
                    .checked_sub(metrics.txs_confirmed)
                    .unwrap_or_default();
                metrics.txs_finalized_ps = txs_finalized
                    .checked_sub(metrics.txs_finalized)
                    .unwrap_or_default();

                metrics.txs_sent = txs_sent;
                metrics.txs_confirmed = txs_confirmed;
                metrics.txs_finalized = txs_finalized;

                metrics.mem_used = match procinfo::pid::statm_self() {
                    Ok(statm) => Some(statm.size),
                    Err(err) => {
                        warn!("Error capturing memory consumption {err}");
                        None
                    }
                };

                log::info!("{metrics:?}");
            }
        })
    }
}
