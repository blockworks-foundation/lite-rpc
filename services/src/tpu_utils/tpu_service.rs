use anyhow::Context;

use super::tpu_connection_manager::TpuConnectionManager;
use crate::tpu_utils::quic_proxy_connection_manager::QuicProxyConnectionManager;
use crate::tpu_utils::tpu_connection_path::TpuConnectionPath;
use crate::tpu_utils::tpu_service::ConnectionManager::{DirectTpu, QuicProxy};
use solana_lite_rpc_core::data_cache::DataCache;
use solana_lite_rpc_core::leaders_fetcher_trait::LeaderFetcherInterface;
use solana_lite_rpc_core::quic_connection_utils::QuicConnectionParameters;
use solana_lite_rpc_core::streams::SlotStream;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::{quic::QUIC_PORT_OFFSET, signature::Keypair, slot_history::Slot};
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};
use std::collections::HashMap;
use std::fs::{File, read_to_string};
use std::io::Write;
use std::net::SocketAddr;
use itertools::Itertools;
use solana_sdk::pubkey::Pubkey;
use tokio::time::Duration;

#[derive(Clone, Copy)]
pub struct TpuServiceConfig {
    pub fanout_slots: u64,
    pub number_of_leaders_to_cache: usize,
    pub clusterinfo_refresh_time: Duration,
    pub leader_schedule_update_frequency: Duration,
    pub maximum_transaction_in_queue: usize,
    pub maximum_number_of_errors: usize,
    pub quic_connection_params: QuicConnectionParameters,
    pub tpu_connection_path: TpuConnectionPath,
}

#[derive(Clone)]
pub struct TpuService {
    broadcast_sender: Arc<tokio::sync::broadcast::Sender<(String, Vec<u8>)>>,
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

    pub fn send_transaction(&self, signature: String, transaction: Vec<u8>) -> anyhow::Result<()> {
        self.broadcast_sender.send((signature, transaction))?;
        Ok(())
    }

    // update/reconfigure connections on slot change
    async fn update_quic_connections(
        &self,
        current_slot: Slot,
        estimated_slot: Slot,
    ) -> anyhow::Result<()> {
        let load_slot = if estimated_slot <= current_slot {
            current_slot
        } else if estimated_slot.saturating_sub(current_slot) > 8 {
            estimated_slot - 8
        } else {
            current_slot
        };

        let fanout = self.config.fanout_slots;
        let last_slot = estimated_slot + fanout;

        let cluster_nodes = self.data_cache.cluster_info.cluster_nodes.clone();

        let next_leaders = self
            .leader_schedule
            .get_slot_leaders(load_slot, last_slot)
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


        dump_leaders_to_file(connections_to_keep.values().collect_vec());
        // read_file();


        match &self.connection_manager {
            DirectTpu {
                tpu_connection_manager,
            } => {
                tpu_connection_manager
                    .update_connections(
                        self.broadcast_sender.clone(),
                        connections_to_keep,
                        self.data_cache.identity_stakes.get_stakes().await,
                        self.data_cache.txs.clone(),
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

fn dump_leaders_to_file(leaders: Vec<&SocketAddr>) {
    // will create/truncate file
    // 69.197.20.37:8009
    let mut out_file = File::create("leaders.dat.tmp").unwrap();
    for leader_addr in &leaders {
        write!(out_file, "{}\n", leader_addr).unwrap();
    }
    out_file.flush().unwrap();

    std::fs::rename("leaders.dat.tmp", "leaders.dat").unwrap();
}

