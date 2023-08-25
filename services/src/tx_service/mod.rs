use std::{sync::Arc, time::Duration};

use anyhow::Context;
use solana_lite_rpc_core::{
    data_cache::DataCache, notifications::NotificationSender, AnyhowJoinHandle,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use tokio::sync::mpsc;

use crate::tpu_utils::tpu_service::{TpuService, TpuServiceConfig};

use self::{tx_batch_fwd::TxBatchFwd, tx_replayer::TxReplayer, tx_sender::TxSender};

pub mod tx_batch_fwd;
pub mod tx_replayer;
pub mod tx_sender;

#[derive(Clone, Debug)]
pub struct TxServiceConfig {
    /// TPU identity
    pub identity: Arc<Keypair>,
    /// TPU fanout slots
    pub fanout_slots: u64,
    /// max number of txs in queue backpressure
    pub max_nb_txs_in_queue: usize,
    /// max retries of a tx
    pub max_retries: u16,
    /// retry tx after
    pub retry_after: Duration,
}

pub struct TxService {
    pub data_cache: DataCache,
    // TODO: remove this dependency when get vote accounts is figured out for grpc
    pub rpc_client: Arc<RpcClient>,
    // config
    pub config: TxServiceConfig,
}

impl TxService {
    pub async fn create_tx_services(&self) -> anyhow::Result<(TpuService, TxBatchFwd, TxReplayer)> {
        let TxService {
            data_cache,
            config,
            rpc_client,
        } = self;

        // setup TPU
        let tpu_service = TpuService::new(
            TpuServiceConfig {
                fanout_slots: config.fanout_slots,
                ..Default::default()
            },
            config.identity.clone(),
            rpc_client.clone(),
            data_cache.clone(),
        )
        .await
        .context("Error initializing TPU Service")?;
        // tx batch forwarder to TPU
        let tx_batch_fwd = TxBatchFwd {
            data_cache: data_cache.clone(),
            tpu_service: tpu_service.clone(),
        };
        // tx replayer
        let tx_replayer = TxReplayer {
            tpu_service: tpu_service.clone(),
            data_cache: data_cache.clone(),
            retry_after: config.retry_after,
        };

        Ok((tpu_service, tx_batch_fwd, tx_replayer))
    }

    pub async fn spawn(
        self,
        notifier: Option<NotificationSender>,
    ) -> anyhow::Result<(TxSender, AnyhowJoinHandle)> {
        let (tpu_service, tx_batch_fwd, tx_replayer) = self.create_tx_services().await?;
        // channels
        let (tx_channel, tx_recv) = mpsc::channel(self.config.max_nb_txs_in_queue);
        let (replay_channel, replay_recv) = mpsc::unbounded_channel();

        let tpu_service = tpu_service.start();
        let tx_batch_fwd = tx_batch_fwd.execute(tx_recv, notifier);
        let tx_replayer = tx_replayer.start_service(replay_channel.clone(), replay_recv);

        // spawn
        let jh = tokio::spawn(async move {
            tokio::select! {
                res = tpu_service => {
                    anyhow::bail!("Tpu Service {res:?}")
                },
                res = tx_batch_fwd => {
                    anyhow::bail!("Tx Sender {res:?}")
                },
                res = tx_replayer => {
                    anyhow::bail!("Replay Service {res:?}")
                },
            }
        });

        Ok((
            TxSender {
                tx_channel,
                replay_channel,
                data_cache: self.data_cache,
                max_retries: self.config.max_retries,
                retry_after: self.config.retry_after,
            },
            jh,
        ))
    }
}
