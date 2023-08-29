use super::{tpu_connection_manager::TpuConnectionManager, tpu_service_config::TpuServiceConfig};
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use solana_lite_rpc_core::{
    data_cache::DataCache, leader_schedule::LeaderSchedule, solana_utils::SolanaUtils,
    structures::identity_stakes::IdentityStakes,
};
use solana_rpc_client_api::response::RpcVoteAccountStatus;
use solana_sdk::{pubkey::Pubkey, quic::QUIC_PORT_OFFSET, signature::Keypair, signer::Signer};
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use std::{
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
    sync::Arc,
};
use tokio::{
    sync::{broadcast::Receiver, RwLock},
    time::Instant,
};

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

#[derive(Clone)]
pub struct TpuService {
    data_cache: DataCache,
    broadcast_sender: Arc<tokio::sync::broadcast::Sender<(String, Vec<u8>)>>,
    tpu_connection_manager: Arc<TpuConnectionManager>,
    identity_stakes: Arc<RwLock<IdentityStakes>>,
    leader_schedule: Arc<LeaderSchedule>,
    config: TpuServiceConfig,
    identity: Pubkey,
}

impl TpuService {
    pub async fn new(
        config: TpuServiceConfig,
        identity: Arc<Keypair>,
        leader_schedule: Arc<LeaderSchedule>,
        data_cache: DataCache,
    ) -> anyhow::Result<Self> {
        let (sender, _) = tokio::sync::broadcast::channel(config.maximum_transaction_in_queue);
        let (certificate, key) = new_self_signed_tls_certificate(
            identity.as_ref(),
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        )
        .expect("Failed to initialize QUIC client certificates");

        let tpu_connection_manager =
            TpuConnectionManager::new(certificate, key, config.fanout_slots as usize).await;

        Ok(Self {
            data_cache,
            leader_schedule,
            broadcast_sender: Arc::new(sender),
            tpu_connection_manager: Arc::new(tpu_connection_manager),
            identity_stakes: Arc::new(RwLock::new(IdentityStakes::default())),
            identity: identity.pubkey(),
            config,
        })
    }

    pub async fn update_current_stakes(
        &self,
        rpc_vote_account_streamer: &mut Receiver<RpcVoteAccountStatus>,
    ) -> anyhow::Result<()> {
        // update stakes for identity
        // update stakes for the identity
        {
            let mut lock = self.identity_stakes.write().await;
            *lock = SolanaUtils::get_stakes_for_identity(rpc_vote_account_streamer, self.identity)
                .await?;
        }
        Ok(())
    }

    pub fn send_transaction(&self, signature: String, transaction: Vec<u8>) -> anyhow::Result<()> {
        self.broadcast_sender.send((signature, transaction))?;
        Ok(())
    }

    pub async fn update_leader_schedule(&self) -> anyhow::Result<()> {
        self.leader_schedule
            .update_leader_schedule(
                self.data_cache.slot_cache.get_current_slot(),
                self.data_cache.slot_cache.get_estimated_slot(),
            )
            .await?;
        NB_OF_LEADERS_IN_SCHEDULE.set(self.leader_schedule.len().await as i64);
        Ok(())
    }

    async fn update_quic_connections(&self) {
        let estimated_slot = self.data_cache.slot_cache.get_estimated_slot();
        let current_slot = self.data_cache.slot_cache.get_current_slot();

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

        self.tpu_connection_manager
            .update_connections(
                self.broadcast_sender.clone(),
                connections_to_keep,
                *identity_stakes,
                self.data_cache.clone(),
                self.config.quic_connection_params,
            )
            .await;
    }

    pub async fn start(
        self,
        mut rpc_vote_account_streamer: Receiver<RpcVoteAccountStatus>,
    ) -> anyhow::Result<()> {
        // setup
        self.update_current_stakes(&mut rpc_vote_account_streamer)
            .await?;
        self.update_leader_schedule().await?;
        self.update_quic_connections().await;

        let mut last_cluster_info_update = Instant::now();
        let leader_schedule_update_interval = self.config.leader_schedule_update_frequency;
        let cluster_info_update_interval = self.config.clusterinfo_refresh_time;

        loop {
            tokio::time::sleep(leader_schedule_update_interval).await;

            log::info!("Update leader schedule and cluster nodes");

            if self.update_leader_schedule().await.is_err() {
                log::error!("Unable to update leader schedule");
            }

            if last_cluster_info_update.elapsed() > cluster_info_update_interval {
                if self
                    .update_current_stakes(&mut rpc_vote_account_streamer)
                    .await
                    .is_err()
                {
                    log::error!("Unable to update cluster infos");
                } else {
                    last_cluster_info_update = Instant::now();
                }
            }
        }
    }
}
