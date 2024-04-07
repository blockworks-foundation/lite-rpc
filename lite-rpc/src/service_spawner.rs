use solana_lite_rpc_core::types::BlockInfoStream;
use solana_lite_rpc_core::{
    stores::data_cache::DataCache,
    structures::notifications::NotificationSender,
    types::{BlockStream, ClusterInfoStream, SlotStream, VoteAccountStream},
    AnyhowJoinHandle,
};
use solana_lite_rpc_services::tpu_utils::tpu_connection_path::TpuConnectionPath;
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
use std::net::{SocketAddr, ToSocketAddrs};

pub struct ServiceSpawner {
    pub data_cache: DataCache,
}

impl ServiceSpawner {
    /// spawn services that support the whole system
    pub async fn spawn_support_services(&self, prometheus_addr: String) -> anyhow::Result<()> {
        // spawn prometheus
        let prometheus = PrometheusSync::sync(prometheus_addr.clone());

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

    // TODO remove
    pub async fn _spawn_data_caching_service(
        &self,
        block_notifier: BlockStream,
        blockinfo_notifier: BlockInfoStream,
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
            blockinfo_notifier,
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
            self.data_cache.block_information_store.clone(),
            max_retries,
            slot_notifications,
        )
    }
}


pub fn configure_tpu_connection_path(quic_proxy_addr: Option<String>) -> TpuConnectionPath {
    match quic_proxy_addr {
        None => TpuConnectionPath::QuicDirectPath,
        Some(prox_address) => {
            let proxy_socket_addr = parse_host_port(prox_address.as_str()).unwrap();
            TpuConnectionPath::QuicForwardProxyPath {
                // e.g. "127.0.0.1:11111" or "localhost:11111"
                forward_proxy_address: proxy_socket_addr,
            }
        }
    }
}

fn parse_host_port(host_port: &str) -> Result<SocketAddr, String> {
    let addrs: Vec<_> = host_port
        .to_socket_addrs()
        .map_err(|err| format!("Unable to resolve host {host_port}: {err}"))?
        .collect();
    if addrs.is_empty() {
        Err(format!("Unable to resolve host: {host_port}"))
    } else if addrs.len() > 1 {
        Err(format!("Multiple addresses resolved for host: {host_port}"))
    } else {
        Ok(addrs[0])
    }
}