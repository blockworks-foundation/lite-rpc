use anyhow::{bail, Context};
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use solana_streamer::nonblocking::quic::ConnectionPeerType;

use super::tpu_connection_manager::TpuConnectionManager;
use crate::tpu_utils::tpu_connection_path::TpuConnectionPath;
use crate::tpu_utils::tpu_service::ConnectionManager::DirectTpu;

use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_lite_rpc_core::structures::transaction_sent_info::SentTransactionInfo;
use solana_lite_rpc_core::traits::leaders_fetcher_interface::LeaderFetcherInterface;
use solana_lite_rpc_core::types::SlotStream;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::{quic::QUIC_PORT_OFFSET, signature::Keypair, slot_history::Slot};
use solana_streamer::nonblocking::quic::compute_max_allowed_uni_streams;
use solana_streamer::tls_certificates::new_dummy_x509_certificate;
use std::collections::HashSet;
use std::sync::Arc;

lazy_static::lazy_static! {
    static ref NB_CLUSTER_NODES: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_nb_cluster_nodes", "Number of cluster nodes in saved")).unwrap();

    static ref NB_OF_LEADERS_IN_SCHEDULE: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_cached_leader", "Number of leaders in schedule cache")).unwrap();

    static ref ESTIMATED_SLOT: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_estimated_slot", "Estimated slot seen by last rpc")).unwrap();
}

#[derive(Clone, Copy)]
pub struct TpuServiceConfig {
    pub fanout_slots: u64,
    pub maximum_transaction_in_queue: usize,
    pub tpu_connection_path: TpuConnectionPath,
}

#[derive(Clone)]
pub struct TpuService {
    broadcast_sender: std::sync::mpsc::Sender<SentTransactionInfo>,
    connection_manager: ConnectionManager,
    leader_schedule: Arc<dyn LeaderFetcherInterface>,
    config: TpuServiceConfig,
    data_cache: DataCache,
}

#[derive(Clone)]
enum ConnectionManager {
    DirectTpu {
        tpu_connection_manager: Arc<TpuConnectionManager>,
    },
}

impl TpuService {
    pub async fn new(
        config: TpuServiceConfig,
        identity: Arc<Keypair>,
        leader_schedule: Arc<dyn LeaderFetcherInterface>,
        data_cache: DataCache,
    ) -> anyhow::Result<Self> {
        let (sender, reciever) = std::sync::mpsc::channel::<SentTransactionInfo>();
        let (certificate, key) = new_dummy_x509_certificate(identity.as_ref());

        let identity_stakes = data_cache.identity_stakes.get_stakes().await;

        let number_of_streams = compute_max_allowed_uni_streams(
            identity_stakes.peer_type,
            identity_stakes.total_stakes,
        );
        log::info!(
            "Number of streams for TPU connections: {}",
            number_of_streams
        );

        let connection_manager = match config.tpu_connection_path {
            TpuConnectionPath::QuicDirectPath => {
                let tpu_connection_manager =
                    TpuConnectionManager::new(certificate, key, number_of_streams as u64, reciever)
                        .await;
                DirectTpu {
                    tpu_connection_manager: Arc::new(tpu_connection_manager),
                }
            }
            TpuConnectionPath::QuicForwardProxyPath { .. } => {
                bail!("QuicForwardProxyPath is not supported");
            }
        };

        Ok(Self {
            leader_schedule,
            broadcast_sender: sender,
            connection_manager,
            config,
            data_cache,
        })
    }

    pub fn send_transaction(&self, transaction: &SentTransactionInfo) -> anyhow::Result<()> {
        self.broadcast_sender.send(transaction.clone())?;
        Ok(())
    }

    // update/reconfigure connections on slot change
    async fn update_quic_connections(
        &self,
        current_slot: Slot,
        estimated_slot: Slot,
    ) -> anyhow::Result<()> {
        let fanout = self.config.fanout_slots;
        let last_slot = estimated_slot + fanout;
        let current_slot = current_slot.saturating_sub(4);

        let cluster_nodes = self.data_cache.cluster_info.cluster_nodes.clone();

        let next_leaders = self
            .leader_schedule
            .get_slot_leaders(current_slot, last_slot)
            .await?;

        let identity_stakes = self.data_cache.identity_stakes.get_stakes().await;

        let enable_tpu_forwards = {
            match identity_stakes.peer_type {
                ConnectionPeerType::Unstaked => false,
                ConnectionPeerType::Staked(_) => true,
            }
        };
        // get next leader with its tpu port
        let connections_to_keep: HashSet<(_, _)> = next_leaders
            .iter()
            .map(|x| {
                let contact_info = cluster_nodes.get(&x.pubkey);
                let tpu_port = match contact_info {
                    Some(info) => info.tpu,
                    _ => None,
                };
                (x.pubkey, tpu_port)
            })
            .filter(|x| x.1.is_some())
            .flat_map(|x| {
                let mut addresses = vec![];
                let mut tpu_addr = x.1.unwrap();
                // add quic port offset
                tpu_addr.set_port(tpu_addr.port() + QUIC_PORT_OFFSET);

                addresses.push((x.0, tpu_addr));

                if enable_tpu_forwards {
                    // Technically the forwards port could be anywhere and unfortunately getClusterNodes
                    // does not report it. However it's nearly always directly after the tpu port.
                    let mut tpu_forwards_addr = tpu_addr;
                    tpu_forwards_addr.set_port(tpu_addr.port() + 1);
                    addresses.push((x.0, tpu_forwards_addr));
                }

                addresses
            })
            .collect();
        let connections = connections_to_keep.iter().map(|x| x.1).collect::<Vec<_>>();

        match &self.connection_manager {
            DirectTpu {
                tpu_connection_manager,
            } => {
                tpu_connection_manager.update_connections(connections).await;
            }
        }
        Ok(())
    }

    pub fn start(&self, slot_notifications: SlotStream) -> AnyhowJoinHandle {
        let this = self.clone();
        tokio::spawn(async move {
            let mut slot_notifications = slot_notifications;
            loop {
                let notification = slot_notifications
                    .recv()
                    .await
                    .context("Tpu service cannot get slot notification")?;
                this.update_quic_connections(
                    notification.processed_slot,
                    notification.estimated_processed_slot,
                )
                .await?;
            }
        })
    }
}
