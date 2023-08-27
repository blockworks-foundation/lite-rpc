use solana_lite_rpc_core::{
    data_cache::DataCache, leader_schedule::LeaderSchedule, notifications::NotificationSender,
    AnyhowJoinHandle,
};
use solana_rpc_client_api::response::RpcVoteAccountStatus;
use std::{sync::Arc, time::Duration};
use tokio::sync::broadcast::Receiver;

use crate::{
    cleaner::Cleaner,
    metrics_capture::MetricsCapture,
    prometheus_sync::PrometheusSync,
    tx_service::{tx_sender::TxSender, TxService, TxServiceConfig},
};

pub struct Spawner {
    pub prometheus_addr: String,
    // listener addr
    pub addr: String,
    // tx services config
    pub tx_service_config: TxServiceConfig,
    // this is temporary, we will remove this when we have a way to get vote accounts from grpc
    pub rpc_addr: String,
    // internal
    pub data_cache: DataCache,
    pub notification_channel: Option<NotificationSender>,
}

impl Spawner {
    /// spawn services that support the whole system
    pub async fn spawn_support_services(&self) -> anyhow::Result<()> {
        // spawn prometheus
        let prometheus = PrometheusSync::sync(self.prometheus_addr.clone());

        // spawn metrics capture
        let metrics = MetricsCapture::new(self.data_cache.clone()).capture();

        // spawn cleaner
        // transactions get invalid in around 1 mins, because the block hash expires in 150 blocks so 150 * 400ms = 60s
        // Setting it to two to give some margin of error / as not all the blocks are filled.
        let cleaner = Cleaner {
            data_cache: self.data_cache.clone(),
        }
        .start(Duration::from_secs(120));

        tokio::select! {
            prometheus_res = prometheus => {
                anyhow::bail!("Prometheus exited unexpectedly: {prometheus_res:?}");
            }
            metrics_res = metrics => {
                anyhow::bail!("Metrics capture exited unexpectedly: {metrics_res:?}");
            }
            cleaner_res = cleaner => {
                anyhow::bail!("Cleaner exited unexpectedly: {cleaner_res:?}");
            }
        }
    }

    pub async fn spawn_tx_service(
        &self,
        leader_schedule: Arc<LeaderSchedule>,
        rpc_vote_account_streamer: Receiver<RpcVoteAccountStatus>,
    ) -> anyhow::Result<(TxSender, AnyhowJoinHandle)> {
        TxService {
            data_cache: self.data_cache.clone(),
            config: self.tx_service_config.clone(),
            leader_schedule,
        }
        .spawn(rpc_vote_account_streamer, self.notification_channel.clone())
        .await
    }
}
