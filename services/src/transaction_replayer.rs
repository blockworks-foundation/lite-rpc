use crate::tpu_utils::tpu_service::TpuService;
use anyhow::{bail, Context};
use log::{error, info};
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use solana_lite_rpc_core::{
    stores::data_cache::DataCache, structures::transaction_sent_info::SentTransactionInfo,
    AnyhowJoinHandle,
};
use std::time::Duration;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    time::Instant,
};

lazy_static::lazy_static! {
    pub static ref MESSAGES_IN_REPLAY_QUEUE: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_messages_in_replay_queue", "Number of transactions waiting for replay")).unwrap();
}

#[derive(Debug, Clone)]
pub struct TransactionReplay {
    pub transaction: SentTransactionInfo,
    pub replay_count: usize,
    pub max_replay: usize,
    pub replay_at: Instant,
}

/// Transaction Replayer
/// It will replay transaction sent to the cluster if they are not confirmed
/// They will be replayed max_replay times
/// The replay time will be linearly increasing by after count * replay after
/// So the transasctions will be replayed like retry_after, retry_after*2, retry_after*3 ...

#[derive(Clone)]
pub struct TransactionReplayer {
    pub tpu_service: TpuService,
    pub data_cache: DataCache,
    pub retry_offset: Duration,
}

impl TransactionReplayer {
    pub fn new(tpu_service: TpuService, data_cache: DataCache, retry_offset: Duration) -> Self {
        Self {
            tpu_service,
            data_cache,
            retry_offset,
        }
    }

    pub fn start_service(
        &self,
        sender: UnboundedSender<TransactionReplay>,
        mut receiver: UnboundedReceiver<TransactionReplay>,
    ) -> AnyhowJoinHandle {
        let tpu_service = self.tpu_service.clone();
        let data_cache = self.data_cache.clone();
        let retry_offset = self.retry_offset;

        info!("replay inner spawn");
        tokio::spawn(async move {

            info!("async moved");
            while let Some(mut tx_replay) = receiver.recv().await {
                MESSAGES_IN_REPLAY_QUEUE.dec();
                let now = Instant::now();
                if now < tx_replay.replay_at {
                    if tx_replay.replay_at > now + retry_offset {
                        // requeue the transactions will be replayed after retry_after duration
                        sender.send(tx_replay).context("replay channel closed")?;
                        MESSAGES_IN_REPLAY_QUEUE.inc();
                        continue;
                    }
                    tokio::time::sleep_until(tx_replay.replay_at).await;
                }
                if data_cache.check_if_confirmed_or_expired_blockheight(&tx_replay.transaction) {
                    // transaction has already expired or confirmed
                    continue;
                }
                // ignore reset error
                let _ = tpu_service.send_transaction(&tx_replay.transaction);

                if tx_replay.replay_count < tx_replay.max_replay {
                    tx_replay.replay_count += 1;
                    tx_replay.replay_at =
                        Instant::now() + retry_offset.mul_f32(tx_replay.replay_count as f32);
                    sender.send(tx_replay).context("replay channel closed")?;
                    MESSAGES_IN_REPLAY_QUEUE.inc();
                }
            }
            error!("transaction replay channel broken");
            bail!("transaction replay channel broken");
        })
    }
}
