use crate::tpu_utils::tpu_service::TpuService;
use anyhow::bail;
use log::error;
use solana_lite_rpc_core::{tx_store::TxStore, AnyhowJoinHandle};
use std::time::Duration;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    time::Instant,
};

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
    pub tpu_service: TpuService,
    pub tx_store: TxStore,
    pub retry_after: Duration,
}

impl TransactionReplayer {
    pub fn new(tpu_service: TpuService, tx_store: TxStore, retry_after: Duration) -> Self {
        Self {
            tpu_service,
            tx_store,
            retry_after,
        }
    }

    pub fn start_service(
        &self,
        sender: UnboundedSender<TransactionReplay>,
        mut reciever: UnboundedReceiver<TransactionReplay>,
    ) -> AnyhowJoinHandle {
        let tpu_service = self.tpu_service.clone();
        let tx_store = self.tx_store.clone();
        let retry_after = self.retry_after;

        tokio::spawn(async move {
            while let Some(mut tx_replay) = reciever.recv().await {
                if Instant::now() < tx_replay.replay_at {
                    tokio::time::sleep_until(tx_replay.replay_at).await;
                }
                if let Some(tx) = tx_store.get(&tx_replay.signature) {
                    if tx.status.is_some() {
                        // transaction has been confirmed / no retry needed
                        continue;
                    }
                } else {
                    // transaction timed out
                    continue;
                }
                // ignore reset error
                let _ =
                    tpu_service.send_transaction(tx_replay.signature.clone(), tx_replay.tx.clone());

                if tx_replay.replay_count < tx_replay.max_replay {
                    tx_replay.replay_count += 1;
                    tx_replay.replay_at = Instant::now() + retry_after;
                    if let Err(e) = sender.send(tx_replay) {
                        error!("error while scheduling replay ({})", e);
                        continue;
                    }
                }
            }
            error!("transaction replay channel broken");
            bail!("transaction replay channel broken");
        })
    }
}
