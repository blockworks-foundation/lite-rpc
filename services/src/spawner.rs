use std::{sync::Arc, time::Duration};

use solana_lite_rpc_core::{
    data_cache::DataCache, notifications::NotificationSender, AnyhowJoinHandle,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

use crate::{
    cleaner::Cleaner,
    ledger_service::{GrpcLedgerProvider, LedgerService, RpcLedgerProvider},
    metrics_capture::MetricsCapture,
    prometheus_sync::PrometheusSync,
    tx_service::{tx_sender::TxSender, TxService, TxServiceConfig},
};

pub struct Spawner {
    pub prometheus_addr: String,
    // use grpc or rpc
    pub grpc: bool,
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
    /// spawn data aggrigators to ledger
    pub async fn spawn_ledger_service(&self) -> anyhow::Result<()> {
        // TODO: add error loop
        if self.grpc {
            LedgerService::<GrpcLedgerProvider>::from(self.data_cache.clone())
                .listen(self.addr.clone())
                .await
        } else {
            LedgerService::<RpcLedgerProvider>::from(self.data_cache.clone())
                .listen(self.addr.clone())
                .await
        }
    }

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

    pub async fn spawn_tx_service(&self) -> anyhow::Result<(TxSender, AnyhowJoinHandle)> {
        TxService {
            data_cache: self.data_cache.clone(),
            config: self.tx_service_config.clone(),
            rpc_client: Arc::new(RpcClient::new(self.rpc_addr.clone())),
        }
        .spawn(self.notification_channel.clone())
        .await
    }
}
