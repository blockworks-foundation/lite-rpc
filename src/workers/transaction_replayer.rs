use super::TxSender;
use log::error;
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use std::time::Duration;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
    time::Instant,
};

lazy_static::lazy_static! {
    pub static ref MESSAGES_IN_REPLAY_QUEUE: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_messages_in_replay_queue", "Number of quic connections open")).unwrap();
}

#[derive(Debug, Clone)]
pub struct TransactionReplay {
    pub signature: String,
    pub tx: Vec<u8>,
    pub replay_count: usize,
    pub max_replay: usize,
    pub replay_at: Instant,
}

#[derive(Clone)]
pub struct TransactionReplayer {
    pub tx_sender: TxSender,
    pub retry_after: Duration,
}

impl TransactionReplayer {
    pub fn new(tx_sender: TxSender, retry_after: Duration) -> Self {
        Self {
            tx_sender,
            retry_after,
        }
    }

    pub fn start_service(
        &self,
        sender: UnboundedSender<TransactionReplay>,
        reciever: UnboundedReceiver<TransactionReplay>,
    ) -> JoinHandle<anyhow::Result<()>> {
        let tx_sender = self.tx_sender.clone();
        let retry_after = self.retry_after;
        tokio::spawn(async move {
            let mut reciever = reciever;
            loop {
                let tx = reciever.recv().await;
                match tx {
                    Some(mut tx_replay) => {
                        MESSAGES_IN_REPLAY_QUEUE.dec();
                        if Instant::now() < tx_replay.replay_at {
                            tokio::time::sleep_until(tx_replay.replay_at).await;
                        }
                        if let Some(tx) = tx_sender.txs_sent_store.get(&tx_replay.signature) {
                            if tx.status.is_some() {
                                // transaction has been confirmed / no retry needed
                                continue;
                            }
                        } else {
                            // transaction timed out
                            continue;
                        }
                        // ignore reset error
                        let _ = tx_sender.tpu_service.send_transaction(tx_replay.tx.clone());

                        if tx_replay.replay_count < tx_replay.max_replay {
                            tx_replay.replay_count += 1;
                            tx_replay.replay_at = Instant::now() + retry_after;
                            if let Err(e) = sender.send(tx_replay) {
                                error!("error while scheduling replay ({})", e);
                                continue;
                            } else {
                                MESSAGES_IN_REPLAY_QUEUE.inc();
                            }
                        }
                    }
                    None => {
                        error!("transaction replay channel broken");
                        break;
                    }
                }
            }
            Ok(())
        })
    }
}
