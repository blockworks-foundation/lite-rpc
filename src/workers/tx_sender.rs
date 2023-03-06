use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::bail;
use dashmap::DashMap;
use log::{info, warn};

use prometheus::{core::GenericGauge, opts, register_int_counter, register_int_gauge, IntCounter};
use solana_transaction_status::TransactionStatus;
use tokio::{sync::mpsc::UnboundedReceiver, task::JoinHandle};

use crate::{
    bridge::TXS_IN_CHANNEL,
    tpu_manager::TpuManager,
    workers::{PostgresMsg, PostgresTx, MESSAGES_IN_POSTGRES_CHANNEL},
};

use super::PostgresMpscSend;

lazy_static::lazy_static! {
    static ref TXS_SENT: IntCounter =
        register_int_counter!("literpc_txs_sent", "Number of transactions forwarded to tpu").unwrap();
    static ref TXS_SENT_ERRORS: IntCounter =
    register_int_counter!("literpc_txs_sent_errors", "Number of errors while transactions forwarded to tpu").unwrap();
    static ref TX_BATCH_SIZES: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_tx_batch_size", "batchsize of tx sent by literpc")).unwrap();
}

pub type WireTransaction = Vec<u8>;

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
    ) {
        assert_eq!(sigs_and_slots.len(), txs.len());

        if sigs_and_slots.is_empty() {
            return;
        }

        let tpu_client = self.tpu_manager.clone();
        let txs_sent = self.txs_sent_store.clone();

        let quic_response = match tpu_client.try_send_wire_transaction_batch(txs).await {
            Ok(_) => {
                for (sig, _) in &sigs_and_slots {
                    txs_sent.insert(sig.to_owned(), TxProps::default());
                }
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

        if let Some(postgres) = postgres {
            let forwarded_slot = tpu_client.estimated_current_slot().await;

            for (sig, recent_slot) in sigs_and_slots {
                MESSAGES_IN_POSTGRES_CHANNEL.inc();
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
    }

    /// retry and confirm transactions every 2ms (avg time to confirm tx)
    pub fn execute(
        self,
        mut recv: UnboundedReceiver<(String, WireTransaction, u64)>,
        tx_batch_size: usize,
        tx_send_interval: Duration,
        postgres_send: Option<PostgresMpscSend>,
    ) -> JoinHandle<anyhow::Result<()>> {
        let (batch_send, batch_recv) = async_channel::unbounded();

        for _i in 0..5 {
            let this = self.clone();
            let batch_recv = batch_recv.clone();
            let postgres_send = postgres_send.clone();

            tokio::spawn(async move {
                while let Ok((sigs_and_slots, txs)) = batch_recv.recv().await {
                    this.forward_txs(sigs_and_slots, txs, postgres_send.clone())
                        .await;
                }
            });
        }

        tokio::spawn(async move {
            info!(
                "Batching tx(s) with batch size of {tx_batch_size} every {}ms",
                tx_send_interval.as_millis()
            );

            loop {
                let mut sigs_and_slots = Vec::with_capacity(tx_batch_size);
                let mut txs = Vec::with_capacity(tx_batch_size);

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
                            break;
                        }
                    }
                }
                TX_BATCH_SIZES.set(txs.len() as i64);
                batch_send.send((sigs_and_slots, txs)).await?;
            }
        })
    }
}
