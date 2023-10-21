use anyhow::Context;
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use solana_lite_rpc_core::structures::transaction_sent_info::SentTransactionInfo;

use super::tpu_connection_manager::TpuConnectionManager;
use crate::tpu_utils::quic_proxy_connection_manager::QuicProxyConnectionManager;
use crate::tpu_utils::tpu_connection_path::TpuConnectionPath;
use crate::tpu_utils::tpu_service::ConnectionManager::{DirectTpu, QuicProxy};
use solana_lite_rpc_core::quic_connection_utils::QuicConnectionParameters;
use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_lite_rpc_core::traits::leaders_fetcher_interface::LeaderFetcherInterface;
use solana_lite_rpc_core::types::SlotStream;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::{quic::QUIC_PORT_OFFSET, signature::Keypair, slot_history::Slot};
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use itertools::Itertools;
use log::info;
use solana_sdk::pubkey::Pubkey;
use solana_lite_rpc_core::structures::leader_data::LeaderData;

lazy_static::lazy_static! {
    static ref NB_CLUSTER_NODES: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_nb_cluster_nodes", "Number of cluster nodes in saved")).unwrap();

    static ref NB_OF_LEADERS_IN_SCHEDULE: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_cached_leader", "Number of leaders in schedule cache")).unwrap();

    static ref CURRENT_SLOT: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_current_slot", "Current slot seen by last rpc")).unwrap();

    static ref ESTIMATED_SLOT: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_estimated_slot", "Estimated slot seen by last rpc")).unwrap();
}

#[derive(Clone, Copy)]
pub struct TpuServiceConfig {
    pub fanout_slots: u64,
    pub maximum_transaction_in_queue: usize,
    pub quic_connection_params: QuicConnectionParameters,
    pub tpu_connection_path: TpuConnectionPath,
}

#[derive(Clone)]
pub struct TpuService {
    broadcast_sender: Arc<tokio::sync::broadcast::Sender<SentTransactionInfo>>,
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
    QuicProxy {
        quic_proxy_connection_manager: Arc<QuicProxyConnectionManager>,
    },
}

impl TpuService {
    pub async fn new(
        config: TpuServiceConfig,
        identity: Arc<Keypair>,
        leader_schedule: Arc<dyn LeaderFetcherInterface>,
        data_cache: DataCache,
    ) -> anyhow::Result<Self> {
        let (sender, _) = tokio::sync::broadcast::channel(config.maximum_transaction_in_queue);
        let (certificate, key) = new_self_signed_tls_certificate(
            identity.as_ref(),
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        )
        .expect("Failed to initialize QUIC client certificates");

        let connection_manager = match config.tpu_connection_path {
            TpuConnectionPath::QuicDirectPath => {
                let tpu_connection_manager =
                    TpuConnectionManager::new(certificate, key, config.fanout_slots as usize).await;
                DirectTpu {
                    tpu_connection_manager: Arc::new(tpu_connection_manager),
                }
            }
            TpuConnectionPath::QuicForwardProxyPath {
                forward_proxy_address,
            } => {
                let quic_proxy_connection_manager =
                    QuicProxyConnectionManager::new(certificate, key, forward_proxy_address).await;

                QuicProxy {
                    quic_proxy_connection_manager: Arc::new(quic_proxy_connection_manager),
                }
            }
        };

        Ok(Self {
            leader_schedule,
            broadcast_sender: Arc::new(sender),
            connection_manager,
            config,
            data_cache,
        })
    }

    pub fn send_transaction(&self, transaction: &SentTransactionInfo) -> anyhow::Result<()> {
        self.broadcast_sender.send(transaction.clone())?;
        Ok(())
    }

    async fn ping_tpus_by_leader_schedule(
        &self,
        slot: Slot
    ) -> anyhow::Result<()> {
        let cluster_nodes = self.data_cache.cluster_info.cluster_nodes.clone();

        let leader_pingtpu_list = self
            .leader_schedule
            .get_slot_leaders(slot, slot + 5000) // 5000 = leaders to cache
            .await?
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
            .map(|x| {
                let mut addr = x.1.unwrap();
                // add quic port offset
                addr.set_port(addr.port() + QUIC_PORT_OFFSET);
                (x.0, addr)
            })
            .unique()
            .collect_vec();

        // disclaimer - do not know how expensive that is
        // run once!
        tokio::spawn(async move {
            ping_tpus(&leader_pingtpu_list).await;
        });

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

        let cluster_nodes = self.data_cache.cluster_info.cluster_nodes.clone();

        let next_leaders = self
            .leader_schedule
            .get_slot_leaders(current_slot, last_slot)
            .await?;
        // get next leader with its tpu port
        let connections_to_keep: HashMap<Pubkey, SocketAddr> = next_leaders
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
            .map(|x| {
                let mut addr = x.1.unwrap();
                // add quic port offset
                addr.set_port(addr.port() + QUIC_PORT_OFFSET);
                (x.0, addr)
            })
            .collect();

        match &self.connection_manager {
            DirectTpu {
                tpu_connection_manager,
            } => {
                tpu_connection_manager
                    .update_connections(
                        self.broadcast_sender.clone(),
                        connections_to_keep,
                        self.data_cache.identity_stakes.get_stakes().await,
                        self.data_cache.clone(),
                        self.config.quic_connection_params,
                    )
                    .await;
            }
            QuicProxy {
                quic_proxy_connection_manager,
            } => {
                let transaction_receiver = self.broadcast_sender.subscribe();
                quic_proxy_connection_manager
                    .update_connection(
                        transaction_receiver,
                        connections_to_keep,
                        self.config.quic_connection_params,
                    )
                    .await;
            }
        }
        Ok(())
    }

    pub fn start(&self, slot_notifications: SlotStream) -> AnyhowJoinHandle {
        let this = self.clone();

        tokio::spawn(async move {
            let mut slot_notifications = slot_notifications;

            this.ping_tpus_by_leader_schedule(slot_notifications.recv().await.unwrap().processed_slot).await?;

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

async fn ping_tpus(leaders: &Vec<(Pubkey, SocketAddr)>) {

    // truncate for testing
    // let leaders = &leaders[0..3];

    let timeout = Duration::from_millis(1500);
    // TODO run at least 3x and calculate average and deviation
    let mut quicping_stats: Vec<(Pubkey, SocketAddr, Duration)> = vec![];

    info!("Send out pings to {} leaders...", leaders.len());
    // TODO tokio
    for (pubkey, socket_addr) in leaders {
        // Mango Validator mainnet - 202.8.9.108:8009

        let elapsed = quinn_quicping::pingo::quicping(*socket_addr, Some(timeout));

        quicping_stats.push((*pubkey, *socket_addr, elapsed));

        info!("quic-ping to {} ({}): {:.3}ms", socket_addr, pubkey, elapsed.as_secs_f64() * 1000.0);
    }
    info!("Done pinging leaders");

    info!("dump CSV....");
    quicping_stats.sort_by(|lhs, rhs| lhs.2.cmp(&rhs.2).reverse());

    for stat in quicping_stats {
        if stat.2 < timeout {
            println!("CSV {}\t{}\t{}\t{}", stat.0, stat.1.ip(), stat.1.port(), (stat.2.as_secs_f64() * 1000.0) as u64);
        } else {
            println!("CSV {}\t{}\t{}\tTIMEOUT", stat.0, stat.1.ip(), stat.1.port());
        }
    }

    info!("dump CSV..done");



}
