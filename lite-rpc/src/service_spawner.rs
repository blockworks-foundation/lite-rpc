use solana_lite_rpc_core::{
    stores::data_cache::DataCache,
    structures::notifications::NotificationSender,
    types::{BlockStream, ClusterInfoStream, SlotStream, VoteAccountStream},
    AnyhowJoinHandle,
};
use solana_lite_rpc_services::{
    data_caching_service::DataCachingService,
    metrics_capture::MetricsCapture,
    prometheus_sync::PrometheusSync,
    tpu_utils::tpu_service::TpuService,
    transaction_replayer::TransactionReplayer,
    transaction_service::{TransactionService, TransactionServiceBuilder},
    tx_sender::TxSender,
};
use std::time::Duration;
pub struct ServiceSpawner {
    pub prometheus_addr: String,
    pub data_cache: DataCache,
}

impl ServiceSpawner {
    /// spawn services that support the whole system
    pub async fn spawn_support_services(&self) -> anyhow::Result<()> {
        // spawn prometheus
        let prometheus = PrometheusSync::sync(self.prometheus_addr.clone());

        // spawn metrics capture
        let metrics = MetricsCapture::new(self.data_cache.txs.clone()).capture();

        tokio::select! {
            prometheus_res = prometheus => {
                anyhow::bail!("Prometheus exited unexpectedly: {prometheus_res:?}");
            }
            metrics_res = metrics => {
                anyhow::bail!("Metrics capture exited unexpectedly: {metrics_res:?}");
            }
        }
    }

    pub async fn spawn_data_caching_service(
        &self,
        block_notifier: BlockStream,
        slot_notification: SlotStream,
        cluster_info_notification: ClusterInfoStream,
        va_notification: VoteAccountStream,
    ) -> Vec<AnyhowJoinHandle> {
        let data_service = DataCachingService {
            data_cache: self.data_cache.clone(),
            clean_duration: Duration::from_secs(120),
        };

        data_service.listen(
            block_notifier,
            slot_notification,
            cluster_info_notification,
            va_notification,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn_tx_service(
        &self,
        tx_sender: TxSender,
        tx_replayer: TransactionReplayer,
        tpu_service: TpuService,
        max_nb_txs_in_queue: usize,
        notifier: Option<NotificationSender>,
        max_retries: usize,
        slot_notifications: SlotStream,
    ) -> (TransactionService, AnyhowJoinHandle) {
        let service_builder = TransactionServiceBuilder::new(
            tx_sender,
            tx_replayer,
            tpu_service,
            max_nb_txs_in_queue,
        );
        service_builder.start(
            notifier,
            self.data_cache.block_store.clone(),
            max_retries,
            slot_notifications,
        )
    }
}
