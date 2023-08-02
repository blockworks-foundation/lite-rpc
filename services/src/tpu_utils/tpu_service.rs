use anyhow::{bail, Context};
use log::{error, info};
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use solana_client::nonblocking::rpc_client::RpcClient;

use solana_lite_rpc_core::{
    leader_schedule::LeaderSchedule, quic_connection_utils::QuicConnectionParameters,
    solana_utils::SolanaUtils, structures::identity_stakes::IdentityStakes, tx_store::TxStore,
    AnyhowJoinHandle,
};

use super::tpu_connection_manager::TpuConnectionManager;
use solana_sdk::{
    pubkey::Pubkey, quic::QUIC_PORT_OFFSET, signature::Keypair, signer::Signer, slot_history::Slot,
};
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use std::{
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{
    sync::RwLock,
    time::{Duration, Instant},
};
use crate::tpu_utils::quic_proxy_connection_manager::QuicProxyConnectionManager;
use crate::tpu_utils::tpu_connection_path::TpuConnectionPath;
use crate::tpu_utils::tpu_service::ConnectionManager::{DirectTpu, QuicProxy};

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
    current_slot: Arc<AtomicU64>,
    estimated_slot: Arc<AtomicU64>,
    rpc_client: Arc<RpcClient>,
    broadcast_sender: Arc<tokio::sync::broadcast::Sender<(String, Vec<u8>)>>,
    connection_manager: ConnectionManager,
    identity_stakes: Arc<RwLock<IdentityStakes>>,
    txs_sent_store: TxStore,
    leader_schedule: Arc<LeaderSchedule>,
    config: TpuServiceConfig,
    identity: Pubkey,
}

#[derive(Clone)]
enum ConnectionManager {
    DirectTpu { tpu_connection_manager: Arc<TpuConnectionManager> },
    QuicProxy { quic_proxy_connection_manager: Arc<QuicProxyConnectionManager> },
}

impl TpuService {
    pub async fn new(
        config: TpuServiceConfig,
        identity: Arc<Keypair>,
        current_slot: Slot,
        rpc_client: Arc<RpcClient>,
        txs_sent_store: TxStore,
    ) -> anyhow::Result<Self> {
        let (sender, _) = tokio::sync::broadcast::channel(config.maximum_transaction_in_queue);
        let (certificate, key) = new_self_signed_tls_certificate(
            identity.as_ref(),
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        )
        .expect("Failed to initialize QUIC client certificates");

        let connection_manager =
            match config.tpu_connection_path {
                TpuConnectionPath::QuicDirectPath => {
                    let tpu_connection_manager =
                        TpuConnectionManager::new(certificate, key,
                                                  config.fanout_slots as usize).await;
                    DirectTpu {
                        tpu_connection_manager: Arc::new(tpu_connection_manager),
                    }
                }
                TpuConnectionPath::QuicForwardProxyPath { forward_proxy_address } => {
                    let quic_proxy_connection_manager =
                        QuicProxyConnectionManager::new(certificate, key,  forward_proxy_address).await;

                    QuicProxy {
                        quic_proxy_connection_manager: Arc::new(quic_proxy_connection_manager),
                    }
                }
            };

        Ok(Self {
            current_slot: Arc::new(AtomicU64::new(current_slot)),
            estimated_slot: Arc::new(AtomicU64::new(current_slot)),
            leader_schedule: Arc::new(LeaderSchedule::new(config.number_of_leaders_to_cache)),
            rpc_client,
            broadcast_sender: Arc::new(sender),
            connection_manager,
            identity_stakes: Arc::new(RwLock::new(IdentityStakes::default())),
            txs_sent_store,
            identity: identity.pubkey(),
            config,
        })
    }

    pub async fn update_current_stakes(&self) -> anyhow::Result<()> {
        // update stakes for identity
        // update stakes for the identity
        {
            let mut lock = self.identity_stakes.write().await;
            *lock = SolanaUtils::get_stakes_for_identity(self.rpc_client.clone(), self.identity)
                .await?;
        }
        Ok(())
    }

    pub fn send_transaction(&self, signature: String, transaction: Vec<u8>) -> anyhow::Result<()> {
        self.broadcast_sender.send((signature, transaction))?;
        Ok(())
    }

    pub async fn update_leader_schedule(&self) -> anyhow::Result<()> {
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let estimated_slot = self.estimated_slot.load(Ordering::Relaxed);
        self.leader_schedule
            .update_leader_schedule(self.rpc_client.clone(), current_slot, estimated_slot)
            .await?;
        NB_OF_LEADERS_IN_SCHEDULE.set(self.leader_schedule.len().await as i64);
        NB_CLUSTER_NODES.set(self.leader_schedule.cluster_nodes_len() as i64);
        Ok(())
    }

    // update/reconfigure connections on slot change
    async fn update_quic_connections(&self) {
        let estimated_slot = self.estimated_slot.load(Ordering::Relaxed);
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let load_slot = if estimated_slot <= current_slot {
            current_slot
        } else if estimated_slot.saturating_sub(current_slot) > 8 {
            estimated_slot - 8
        } else {
            current_slot
        };

        let fanout = self.config.fanout_slots;
        let last_slot = estimated_slot + fanout;

        let next_leaders = self.leader_schedule.get_leaders(load_slot, last_slot).await;
        let connections_to_keep = next_leaders
            .into_iter()
            .filter(|x| x.tpu.is_some())
            .map(|x| {
                let mut addr = x.tpu.unwrap();
                // add quic port offset
                addr.set_port(addr.port() + QUIC_PORT_OFFSET);
                (Pubkey::from_str(x.pubkey.as_str()).unwrap(), addr)
            })
            .collect();

        let identity_stakes = self.identity_stakes.read().await;

        match &self.connection_manager {
            DirectTpu {
                tpu_connection_manager,
            } => {
                tpu_connection_manager
                    .update_connections(
                        self.broadcast_sender.clone(),
                        connections_to_keep,
                        *identity_stakes,
                        self.txs_sent_store.clone(),
                        self.config.quic_connection_params,
                    )
                    .await;
            },
            QuicProxy { quic_proxy_connection_manager } => {
                quic_proxy_connection_manager.update_connection(
                    self.broadcast_sender.clone(),
                    connections_to_keep,
                    self.config.quic_connection_params,
                ).await;
            }
        }
    }

    fn update_current_slot(
        &self,
        update_notifier: tokio::sync::mpsc::UnboundedSender<u64>,
    ) -> AnyhowJoinHandle {
        let current_slot = self.current_slot.clone();
        let rpc_client = self.rpc_client.clone();

        let update_slot = move |slot: u64| {
            if slot > current_slot.load(Ordering::Relaxed) {
                current_slot.store(slot, Ordering::Relaxed);
                CURRENT_SLOT.set(slot as i64);
                let _ = update_notifier.send(slot);
            }
        };
        let max_nb_errors = self.config.maximum_number_of_errors;
        tokio::spawn(async move {
            let mut nb_error = 0;

            while nb_error < max_nb_errors {
                // always loop update the current slots as it is central to working of TPU
                let Err(err) = SolanaUtils::poll_slots(&rpc_client, &update_slot).await else {
                    nb_error = 0;
                    continue;
                };

                nb_error += 1;
                log::info!("Got error while polling slot {}", err);
            }

            error!("Reached max amount of errors to fetch latest slot, exiting poll slot loop");
            bail!("Reached max amount of errors to fetch latest slot, exiting poll slot loop")
        })
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        // setup
        self.leader_schedule
            .load_cluster_info(self.rpc_client.clone())
            .await
            .context("failed to load initial cluster info")?;
        self.update_current_stakes().await?;
        self.update_leader_schedule().await?;
        self.update_quic_connections().await;

        let this = self.clone();
        let update_leader_schedule_service = tokio::spawn(async move {
            let mut last_cluster_info_update = Instant::now();
            let leader_schedule_update_interval = this.config.leader_schedule_update_frequency;
            let cluster_info_update_interval = this.config.clusterinfo_refresh_time;
            loop {
                tokio::time::sleep(leader_schedule_update_interval).await;
                info!("update leader schedule and cluster nodes");

                if this.update_leader_schedule().await.is_err() {
                    error!("unable to update leader shedule");
                }

                if last_cluster_info_update.elapsed() > cluster_info_update_interval {
                    if this.update_current_stakes().await.is_err() {
                        error!("unable to update cluster infos");
                    } else {
                        last_cluster_info_update = Instant::now();
                    }
                }
            }
        });

        let (slot_sender, slot_reciever) = tokio::sync::mpsc::unbounded_channel::<Slot>();

        // Service to poll current slot from upstream rpc
        let slot_poll_service = self.update_current_slot(slot_sender);

        // Service to estimate slots
        let this = self.clone();
        let estimated_slot_service = tokio::spawn(async move {
            let mut slot_update_notifier = slot_reciever;
            loop {
                if SolanaUtils::slot_estimator(
                    &mut slot_update_notifier,
                    this.current_slot.clone(),
                    this.estimated_slot.clone(),
                )
                .await
                {
                    ESTIMATED_SLOT.set(this.estimated_slot.load(Ordering::Relaxed) as i64);
                    this.update_quic_connections().await;
                }
            }
        });

        tokio::select! {
            res = update_leader_schedule_service => {
                error!("Leader update Service {res:?}");
                bail!("Leader update Service {res:?}");
            },
            res = slot_poll_service => {
                error!("Slot Poll Service {res:?}");
                bail!("Slot Poll Service {res:?}");
            },
            res = estimated_slot_service => {
                error!("Estimated slot Service {res:?}");
                bail!("Estimated slot Service {res:?}");
            },
        }
    }

    pub fn get_estimated_slot(&self) -> u64 {
        self.estimated_slot.load(Ordering::Relaxed)
    }

    pub fn get_estimated_slot_holder(&self) -> Arc<AtomicU64> {
        self.estimated_slot.clone()
    }
}
