use anyhow::bail;
use log::{error, info};
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use solana_client::nonblocking::rpc_client::RpcClient;

use solana_lite_rpc_core::{
    leader_schedule::LeaderSchedule, solana_utils::SolanaUtils,
    structures::identity_stakes::IdentityStakes, tx_store::TxStore, AnyhowJoinHandle,
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
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::{
    sync::RwLock,
    time::{Duration, Instant},
};

const CACHE_NEXT_SLOT_LEADERS_PUBKEY_SIZE: usize = 1024; // Save pubkey and contact info of next 1024 leaders in the queue
const CLUSTERINFO_REFRESH_TIME: u64 = 60 * 60; // stakes every 1hrs
const LEADER_SCHEDULE_UPDATE_INTERVAL: u64 = 10; // update leader schedule every 10s
const MAXIMUM_TRANSACTIONS_IN_QUEUE: usize = 200_000;
const MAX_NB_ERRORS: usize = 10;

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

/// service (singleton)
#[derive(Clone)]
pub struct TpuService {
    /// out
    current_slot: Arc<AtomicU64>,
    /// out
    estimated_slot: Arc<AtomicU64>,
    /// in
    fanout_slots: u64,
    /// in
    rpc_client: Arc<RpcClient>,
    /// in
    rpc_ws_address: String,
    /// in
    broadcast_sender: Arc<tokio::sync::broadcast::Sender<(String, Vec<u8>)>>,
    /// in
    tpu_connection_manager: Arc<TpuConnectionManager>,
    /// in
    identity: Arc<Keypair>,
    /// out
    identity_stakes: Arc<RwLock<IdentityStakes>>,
    /// in
    txs_sent_store: TxStore,
    /// out
    leader_schedule: Arc<LeaderSchedule>,
}

impl TpuService {
    pub async fn new(
        current_slot: Slot,
        fanout_slots: u64,
        validator_identity: Arc<Keypair>,
        rpc_client: Arc<RpcClient>,
        rpc_ws_address: String,
        txs_sent_store: TxStore,
    ) -> anyhow::Result<Self> {
        let (sender, _) = tokio::sync::broadcast::channel(MAXIMUM_TRANSACTIONS_IN_QUEUE);
        let (certificate, key) = new_self_signed_tls_certificate(
            validator_identity.as_ref(),
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        )
        .expect("Failed to initialize QUIC connection certificates");

        let tpu_connection_manager =
            TpuConnectionManager::new(certificate, key, fanout_slots as usize);

        Ok(Self {
            current_slot: Arc::new(AtomicU64::new(current_slot)),
            estimated_slot: Arc::new(AtomicU64::new(current_slot)),
            leader_schedule: Arc::new(LeaderSchedule::new(CACHE_NEXT_SLOT_LEADERS_PUBKEY_SIZE)),
            fanout_slots,
            rpc_client,
            rpc_ws_address,
            broadcast_sender: Arc::new(sender),
            tpu_connection_manager: Arc::new(tpu_connection_manager),
            identity: validator_identity,
            identity_stakes: Arc::new(RwLock::new(IdentityStakes::default())),
            txs_sent_store,
        })
    }

    pub async fn update_current_stakes(&self) -> anyhow::Result<()> {
        // update stakes for identity
        // update stakes for the identity
        {
            let mut lock = self.identity_stakes.write().await;
            *lock = SolanaUtils::get_stakes_for_identity(
                self.rpc_client.clone(),
                self.identity.pubkey(),
            )
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

        let fanout = self.fanout_slots;
        let last_slot = estimated_slot + fanout;

        let next_leaders = self.leader_schedule.get_leaders(load_slot, last_slot).await;
        let connections_to_keep = next_leaders
            .iter()
            .filter(|x| x.tpu.is_some())
            .map(|x| {
                let mut addr = x.tpu.unwrap();
                // add quic port offset
                addr.set_port(addr.port() + QUIC_PORT_OFFSET);
                (Pubkey::from_str(x.pubkey.as_str()).unwrap(), addr)
            })
            .collect();

        let identity_stakes = self.identity_stakes.read().await;

        self.tpu_connection_manager
            .update_connections(
                self.broadcast_sender.clone(),
                connections_to_keep,
                *identity_stakes,
                self.txs_sent_store.clone(),
            )
            .await;
    }

    fn check_exit_signal(exit_signal: &Arc<AtomicBool>) -> bool {
        exit_signal.load(Ordering::Relaxed)
    }

    async fn update_current_slot(
        &self,
        update_notifier: tokio::sync::mpsc::UnboundedSender<u64>,
        exit_signal: Arc<AtomicBool>,
    ) {
        let current_slot = self.current_slot.clone();
        let update_slot = |slot: u64| {
            if slot > current_slot.load(Ordering::Relaxed) {
                current_slot.store(slot, Ordering::Relaxed);
                CURRENT_SLOT.set(slot as i64);
                let _ = update_notifier.send(slot);
            }
        };
        let mut nb_errror = 0;
        loop {
            if Self::check_exit_signal(&exit_signal) {
                break;
            }
            // always loop update the current slots as it is central to working of TPU
            if let Err(e) =
                SolanaUtils::poll_slots(self.rpc_client.clone(), &self.rpc_ws_address, update_slot)
                    .await
            {
                nb_errror += 1;
                log::info!("Got error while polling slot {}", e);
                if nb_errror > MAX_NB_ERRORS {
                    error!(
                        "Reached max amount of errors to fetch latest slot, exiting poll slot loop"
                    );
                    break;
                }
            } else {
                nb_errror = 0;
            }
        }
    }

    pub async fn start(&self, exit_signal: Arc<AtomicBool>) -> anyhow::Result<()> {
        self.leader_schedule
            .load_cluster_info(self.rpc_client.clone())
            .await?;
        self.update_current_stakes().await?;
        self.update_leader_schedule().await?;
        self.update_quic_connections().await;

        let this = self.clone();
        let exit_signal_l = exit_signal.clone();
        let jh_update_leaders = tokio::spawn(async move {
            let mut last_cluster_info_update = Instant::now();
            let leader_schedule_update_interval =
                Duration::from_secs(LEADER_SCHEDULE_UPDATE_INTERVAL);
            let cluster_info_update_interval = Duration::from_secs(CLUSTERINFO_REFRESH_TIME);
            loop {
                if Self::check_exit_signal(&exit_signal_l) {
                    break;
                }
                tokio::time::sleep(leader_schedule_update_interval).await;
                if Self::check_exit_signal(&exit_signal_l) {
                    break;
                }

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

        let this = self.clone();
        let (slot_sender, slot_reciever) = tokio::sync::mpsc::unbounded_channel::<Slot>();
        let exit_signal_l = exit_signal.clone();
        let slot_sub_task: AnyhowJoinHandle = tokio::spawn(async move {
            this.update_current_slot(slot_sender, exit_signal_l).await;
            Ok(())
        });

        let estimated_slot = self.estimated_slot.clone();
        let current_slot = self.current_slot.clone();
        let this = self.clone();
        let exit_signal_l = exit_signal.clone();
        let estimated_slot_calculation = tokio::spawn(async move {
            let mut slot_update_notifier = slot_reciever;
            loop {
                if Self::check_exit_signal(&exit_signal_l) {
                    break;
                }

                if SolanaUtils::slot_estimator(
                    &mut slot_update_notifier,
                    current_slot.clone(),
                    estimated_slot.clone(),
                )
                .await
                {
                    ESTIMATED_SLOT.set(estimated_slot.load(Ordering::Relaxed) as i64);
                    this.update_quic_connections().await;
                }
            }
        });

        tokio::select! {
            res = jh_update_leaders => {
                bail!("Leader update service exited unexpectedly {res:?}");
            },
            res = slot_sub_task => {
                bail!("Leader update service exited unexpectedly {res:?}");
            },
            res = estimated_slot_calculation => {
                bail!("Estimated slot calculation service exited unexpectedly {res:?}");
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
