use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::bail;
use dashmap::DashMap;
use log::{info, warn};

use prometheus::{register_counter, Counter};
use solana_transaction_status::TransactionStatus;
use tokio::{
    sync::{mpsc::UnboundedReceiver, TryAcquireError},
    task::JoinHandle,
};

use crate::{
    tpu_manager::TpuManager,
    workers::{PostgresMsg, PostgresTx},
};

use super::PostgresMpscSend;

lazy_static::lazy_static! {
    static ref TXS_SENT: Counter =
        register_counter!("txs_sent", "Number of transactions forwarded to tpu").unwrap();
}

pub type WireTransaction = Vec<u8>;

/// Retry transactions to a maximum of `u16` times, keep a track of confirmed transactions
#[derive(Clone)]
pub struct TxSender {
    /// Tx(s) forwarded to tpu
    pub txs_sent: Arc<DashMap<String, TxProps>>,
    /// TpuClient to call the tpu port
    pub tpu_manager: Arc<TpuManager>,

    counting_semaphore: Arc<tokio::sync::Semaphore>,
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
            txs_sent: Default::default(),
            counting_semaphore: Arc::new(tokio::sync::Semaphore::new(5)),
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
                "Batching tx(s) with batch size of {tx_batch_size} every {} ms",
                tx_send_interval.as_millis()
            );

            loop {
                let mut sigs_and_slots = Vec::with_capacity(tx_batch_size);
                let mut txs = Vec::with_capacity(tx_batch_size);
                let mut maybe_permit = None;
                let counting_semaphore = self.counting_semaphore.clone();
                while txs.len() <= tx_batch_size {
                    let res = tokio::time::timeout(tx_send_interval, recv.recv()).await;
                    match res {
                        Ok(value) => match value {
                            Some((sig, tx, slot)) => {
                                sigs_and_slots.push((sig, slot));
                                txs.push(tx);
                            }
                            None => {
                                bail!("Channel Disconnected");
                            }
                        },
                        _ => {
                            let res = counting_semaphore.clone().try_acquire_owned();
                            match res {
                                Ok(permit) => {
                                    maybe_permit = Some(permit);
                                    break;
                                }
                                Err(TryAcquireError::Closed) => {
                                    bail!("Semaphone permit error");
                                }
                                Err(TryAcquireError::NoPermits) => {
                                    // No permits continue to fetch transactions and batch them
                                }
                            }
                        }
                    }
                }
                assert_eq!(sigs_and_slots.len(), txs.len());

                if sigs_and_slots.is_empty() {
                    continue;
                }

                let permit = match maybe_permit {
                    Some(permit) => permit,
                    None => {
                        // get the permit
                        counting_semaphore.acquire_owned().await.unwrap()
                    }
                };

                let postgres_send = postgres_send.clone();
                let tpu_client = self.tpu_manager.clone();
                let txs_sent = self.txs_sent.clone();
                tokio::spawn(async move {
                    let semaphore_permit = permit;

                    for (sig, _) in &sigs_and_slots {
                        txs_sent.insert(sig.to_owned(), TxProps::default());
                    }
                    info!(
                        "sending {} transactions by tpu size {}",
                        txs.len(),
                        txs_sent.len()
                    );
                    let quic_response = {
                        let _semaphore_permit = semaphore_permit;
                        match tpu_client.try_send_wire_transaction_batch(txs).await {
                            Ok(_) => {
                                // metrics
                                TXS_SENT.inc_by(sigs_and_slots.len() as f64);
                                1
                            }
                            Err(err) => {
                                warn!("{err}");
                                0
                            }
                        }
                    };

                    if let Some(postgres) = postgres_send {
                        let forwarded_slot: u64 = tpu_client.estimated_current_slot().await;

                        for (sig, recent_slot) in sigs_and_slots {
                            postgres
                                .send(PostgresMsg::PostgresTx(PostgresTx {
                                    signature: sig.clone(),
                                    recent_slot: recent_slot as i64,
                                    forwarded_slot: forwarded_slot as i64,
                                    processed_slot: None,
                                    cu_consumed: None,
                                    cu_requested: None,
                                    quic_response,
                                }))
                                .expect("Error writing to postgres service");
                        }
                    }
                });
            }
        })
    }
}
