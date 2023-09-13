use std::sync::Arc;

use log::info;
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use serde::{Deserialize, Serialize};
use solana_lite_rpc_core::stores::tx_store::TxStore;
use solana_transaction_status::TransactionConfirmationStatus;
use tokio::{sync::RwLock, task::JoinHandle};

lazy_static::lazy_static! {
    static ref TXS_IN_STORE: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_txs_in_store", "Transactions in store")).unwrap();
}

#[cfg(all(tokio_unstable, not(loom)))]
#[cfg_attr(docsrs, doc(cfg(tokio_unstable)))]
lazy_static::lazy_static! {
    static ref TOKIO_TASKS: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_tokio_tasks", "Tokio tasks in lite rpc")).unwrap();
    static ref TOKIO_QUEUEDEPTH: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_tokio_blocking_queue_depth", "Tokio tasks in blocking queue in lite rpc")).unwrap();
    static ref TOKIO_INJQUEUEDEPTH: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_tokio_injection_queue_depth", "Tokio tasks in injection queue in lite rpc")).unwrap();
    static ref TOKIO_NB_BLOCKING_THREADS: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_tokio_blocking_threads", "Tokio blocking threads in lite rpc")).unwrap();
    static ref TOKIO_NB_IDLE_THREADS: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_tokio_idle_threads", "Tokio idle threads in lite rpc")).unwrap();
    static ref TOKIO_REMOTE_SCHEDULED_COUNT: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_tokio_remote_scheduled", "Tokio remote scheduled tasks")).unwrap();
    static ref STD_THREADS: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_threads", "Nb of threads used by literpc")).unwrap();
}

/// Background worker which captures metrics
#[derive(Clone)]
pub struct MetricsCapture {
    txs_store: TxStore,
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
    pub fn new(txs_store: TxStore) -> Self {
        Self {
            txs_store,
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

            #[cfg(all(tokio_unstable, not(loom)))]
            #[cfg_attr(docsrs, doc(cfg(tokio_unstable)))]
            info!("Metrics Tokio Unstable enabled");

            loop {
                one_second.tick().await;

                let txs_sent = self.txs_store.len();
                let mut txs_confirmed: usize = 0;
                let mut txs_finalized: usize = 0;

                for tx in self.txs_store.store.iter() {
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

                #[cfg(all(tokio_unstable, not(loom)))]
                #[cfg_attr(docsrs, doc(cfg(tokio_unstable)))]
                {
                    let metrics = tokio::runtime::Handle::current().metrics();
                    TOKIO_TASKS.set(metrics.num_workers() as i64);
                    TOKIO_QUEUEDEPTH.set(metrics.blocking_queue_depth() as i64);
                    TOKIO_NB_BLOCKING_THREADS.set(metrics.num_blocking_threads() as i64);
                    TOKIO_NB_IDLE_THREADS.set(metrics.num_idle_blocking_threads() as i64);
                    TOKIO_INJQUEUEDEPTH.set(metrics.injection_queue_depth() as i64);
                    TOKIO_REMOTE_SCHEDULED_COUNT.set(metrics.remote_schedule_count() as i64);
                }
            }
        })
    }
}
