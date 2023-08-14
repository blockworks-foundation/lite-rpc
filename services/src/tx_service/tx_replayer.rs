use crate::tpu_utils::tpu_service::TpuService;
use anyhow::bail;
use log::error;
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use solana_lite_rpc_core::ledger::Ledger;
use std::time::Duration;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    time::Instant,
};

lazy_static::lazy_static! {
    pub static ref MESSAGES_IN_REPLAY_QUEUE: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_messages_in_replay_queue", "Number of transactions waiting for replay")).unwrap();
}

/// data related to transaction replay request
#[derive(Debug, Clone)]
pub struct TxReplay {
    pub signature: String,
    pub tx: Vec<u8>,
    pub replay_count: u16,
    pub max_replay: u16,
    pub replay_at: Instant,
}

pub struct TxReplayer {
    pub tpu_service: TpuService,
    pub ledger: Ledger,
    pub retry_after: Duration,
}

impl TxReplayer {
    pub async fn start_service(
        self,
        sender: UnboundedSender<TxReplay>,
        mut reciever: UnboundedReceiver<TxReplay>,
    ) -> anyhow::Result<()> {
        while let Some(mut tx_replay) = reciever.recv().await {
            MESSAGES_IN_REPLAY_QUEUE.dec();
            if Instant::now() < tx_replay.replay_at {
                tokio::time::sleep_until(tx_replay.replay_at).await;
            }
            if let Some(tx) = self.ledger.txs.get(&tx_replay.signature) {
                if tx.status.is_some() {
                    // transaction has been confirmed / no retry needed
                    continue;
                }
            } else {
                // transaction timed out
                continue;
            }
            // ignore reset error
            let _ = self
                .tpu_service
                .send_transaction(tx_replay.signature.clone(), tx_replay.tx.clone());

            if tx_replay.replay_count < tx_replay.max_replay {
                tx_replay.replay_count += 1;
                tx_replay.replay_at = Instant::now() + self.retry_after;
                if let Err(e) = sender.send(tx_replay) {
                    error!("error while scheduling replay ({})", e);
                    continue;
                } else {
                    MESSAGES_IN_REPLAY_QUEUE.inc();
                }
            }
        }

        bail!("transaction replay channel broken");
    }
}
