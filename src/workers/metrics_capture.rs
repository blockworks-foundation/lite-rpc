use std::sync::Arc;

use log::info;
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use solana_transaction_status::TransactionConfirmationStatus;
use tokio::{sync::RwLock, task::JoinHandle};

use super::TxSender;
use serde::{Deserialize, Serialize};

lazy_static::lazy_static! {
    static ref TXS_IN_STORE: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_txs_in_store", "Transactions in store")).unwrap();
}

/// Background worker which captures metrics
#[derive(Clone)]
pub struct MetricsCapture {
    tx_sender: TxSender,
    metrics: Arc<RwLock<Metrics>>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Metrics {
    pub txs_sent: usize,
    pub txs_confirmed: usize,
    pub txs_finalized: usize,
    pub txs_ps: usize,
    pub txs_confirmed_ps: usize,
    pub txs_finalized_ps: usize,
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

                let txs_sent = self.tx_sender.txs_sent_store.len();
                let mut txs_confirmed: usize = 0;
                let mut txs_finalized: usize = 0;

                for tx in self.tx_sender.txs_sent_store.iter() {
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
                TXS_IN_STORE.set(txs_sent as i64);
            }
        })
    }
}
