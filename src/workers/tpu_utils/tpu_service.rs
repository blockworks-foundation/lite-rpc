use anyhow::Result;
use dashmap::DashMap;
use futures::StreamExt;
use log::{error, info, warn};
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use solana_client::{
    nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient},
    rpc_response::RpcContactInfo,
};

use solana_sdk::{pubkey::Pubkey, quic::QUIC_PORT_OFFSET, signature::Keypair, slot_history::Slot};
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use std::{
    collections::VecDeque,
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{
    sync::RwLock,
    task::JoinHandle,
    time::{Duration, Instant},
};

use super::tpu_connection_manager::TpuConnectionManager;

const CACHE_NEXT_SLOT_LEADERS_PUBKEY_SIZE: usize = 1024; // Save pubkey and contact info of next 1024 leaders in the queue
const CLUSTERINFO_REFRESH_TIME: u64 = 60; // refresh cluster every minute
const LEADER_SCHEDULE_UPDATE_INTERVAL: u64 = 10; // update leader schedule every 10s
const AVERAGE_SLOT_CHANGE_TIME_IN_MILLIS: u64 = 400;
const MAXIMUM_TRANSACTIONS_IN_QUEUE: usize = 1024;

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

pub struct LeaderData {
    contact_info: Arc<RpcContactInfo>,
    leader_slot: Slot,
}

#[derive(Clone)]
pub struct TpuService {
    cluster_nodes: Arc<DashMap<Pubkey, Arc<RpcContactInfo>>>,
    current_slot: Arc<AtomicU64>,
    estimated_slot: Arc<AtomicU64>,
    leader_schedule: Arc<RwLock<VecDeque<LeaderData>>>,
    fanout_slots: u64,
    rpc_client: Arc<RpcClient>,
    pubsub_client: Arc<PubsubClient>,
    broadcast_sender: Arc<tokio::sync::broadcast::Sender<Vec<u8>>>,
    tpu_connection_manager: Arc<TpuConnectionManager>,
}

impl TpuService {
    pub async fn new(
        current_slot: Arc<AtomicU64>,
        fanout_slots: u64,
        identity: Arc<Keypair>,
        rpc_client: Arc<RpcClient>,
        rpc_ws_address: String,
    ) -> anyhow::Result<Self> {
        let slot = current_slot.load(Ordering::Relaxed);
        let pubsub_client = PubsubClient::new(&rpc_ws_address).await?;
        let (sender, _) = tokio::sync::broadcast::channel(MAXIMUM_TRANSACTIONS_IN_QUEUE);
        let (certificate, key) = new_self_signed_tls_certificate(
            identity.as_ref(),
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        )
        .expect("Failed to initialize QUIC client certificates");

        let tpu_connection_manager =
            TpuConnectionManager::new(certificate, key, fanout_slots as usize);

        Ok(Self {
            cluster_nodes: Arc::new(DashMap::new()),
            current_slot: current_slot,
            estimated_slot: Arc::new(AtomicU64::new(slot)),
            leader_schedule: Arc::new(RwLock::new(VecDeque::new())),
            fanout_slots,
            rpc_client,
            pubsub_client: Arc::new(pubsub_client),
            broadcast_sender: Arc::new(sender),
            tpu_connection_manager: Arc::new(tpu_connection_manager),
        })
    }

    pub async fn update_cluster_nodes(&self) -> Result<()> {
        let cluster_nodes = self.rpc_client.get_cluster_nodes().await?;
        cluster_nodes.iter().for_each(|x| {
            if let Ok(pubkey) = Pubkey::from_str(x.pubkey.as_str()) {
                self.cluster_nodes.insert(pubkey, Arc::new(x.clone()));
            }
        });
        NB_CLUSTER_NODES.set(self.cluster_nodes.len() as i64);
        Ok(())
    }

    pub fn send_transaction(&self, transaction: Vec<u8>) -> anyhow::Result<()> {
        self.broadcast_sender.send(transaction)?;
        Ok(())
    }

    pub async fn update_leader_schedule(&self) -> Result<()> {
        let current_slot = self.current_slot.load(Ordering::Relaxed);

        let (queue_begin_slot, queue_end_slot) = {
            let mut leader_queue = self.leader_schedule.write().await;
            // remove old leaders
            while leader_queue.front().map_or(current_slot, |x| x.leader_slot) < current_slot {
                leader_queue.pop_front();
            }

            let last_element = leader_queue.back().map_or(current_slot, |x| x.leader_slot);
            (current_slot, last_element)
        };

        let last_slot_needed = queue_begin_slot + CACHE_NEXT_SLOT_LEADERS_PUBKEY_SIZE as u64;

        if last_slot_needed > queue_end_slot + 1 {
            let first_slot_to_fetch = queue_end_slot + 1;
            let leaders = self
                .rpc_client
                .get_slot_leaders(first_slot_to_fetch, last_slot_needed - first_slot_to_fetch)
                .await?;

            let mut leader_queue = self.leader_schedule.write().await;
            for i in first_slot_to_fetch..last_slot_needed {
                let current_leader = (i - first_slot_to_fetch) as usize;
                let leader = leaders[current_leader];
                match self.cluster_nodes.get(&leader) {
                    Some(r) => {
                        // push back the leader in the queue
                        leader_queue.push_back(LeaderData {
                            contact_info: r.value().clone(),
                            leader_slot: i,
                        });
                    }
                    None => {
                        warn!("leader not found in cluster info : {}", leader.to_string());
                    }
                }
            }
            NB_OF_LEADERS_IN_SCHEDULE.set(leader_queue.len() as i64);
        }
        Ok(())
    }

    async fn update_quic_connections(&self) {
        let estimated_slot = self.estimated_slot.load(Ordering::Relaxed);
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let load_slot = if estimated_slot <= current_slot {
            current_slot
        } else {
            if estimated_slot - current_slot > 8 {
                estimated_slot - 8
            } else {
                current_slot
            }
        };
        let fanout = self.fanout_slots;

        let next_leaders = {
            let leader_schedule = self.leader_schedule.read().await;
            let mut next_leaders = vec![];
            for leader in leader_schedule.iter() {
                if leader.leader_slot >= load_slot && leader.leader_slot <= load_slot + fanout {
                    next_leaders.push(leader.contact_info.clone());
                } else if leader.leader_slot > load_slot + fanout {
                    break;
                }
            }
            next_leaders
        };
        let connections_to_keep = next_leaders
            .iter()
            .filter(|x| x.tpu.is_some())
            .map(|x| {
                let mut addr = x.tpu.unwrap().clone();
                // add quic port offset
                addr.set_port(addr.port() + QUIC_PORT_OFFSET);
                (Pubkey::from_str(x.pubkey.as_str()).unwrap(), addr)
            })
            .collect();
        self.tpu_connection_manager
            .update_connections(self.broadcast_sender.clone(), connections_to_keep)
            .await;
    }

    pub async fn start(&self) -> anyhow::Result<Vec<JoinHandle<anyhow::Result<()>>>> {
        self.update_cluster_nodes().await?;
        self.update_leader_schedule().await?;
        self.update_quic_connections().await;

        let this = self.clone();
        let jh_update_leaders = tokio::spawn(async move {
            let mut last_cluster_info_update = Instant::now();
            let leader_schedule_update_interval =
                Duration::from_secs(LEADER_SCHEDULE_UPDATE_INTERVAL);
            let cluster_info_update_interval = Duration::from_secs(CLUSTERINFO_REFRESH_TIME);
            loop {
                tokio::time::sleep(leader_schedule_update_interval).await;
                info!("update leader schedule and cluster nodes");
                if this.update_leader_schedule().await.is_err() {
                    error!("unable to update leader shedule");
                }
                if last_cluster_info_update.elapsed() > cluster_info_update_interval {
                    if this.update_cluster_nodes().await.is_err() {
                        error!("unable to update cluster infos");
                    } else {
                        last_cluster_info_update = Instant::now();
                    }
                }
            }
        });

        let pubsub_client = self.pubsub_client.clone();
        let current_slot = self.current_slot.clone();
        let (slot_sender, slot_reciever) = tokio::sync::mpsc::unbounded_channel::<Slot>();

        let slot_sub_task = tokio::spawn(async move {
            let pubsub_client = pubsub_client.clone();
            let current_slot = current_slot.clone();
            loop {
                let res = pubsub_client.slot_subscribe().await;
                if let Ok((mut client, unsub)) = res {
                    loop {
                        let next =
                            tokio::time::timeout(Duration::from_millis(2000), client.next()).await;
                        match next {
                            Ok(slot_info) => {
                                if let Some(slot_info) = slot_info {
                                    if slot_info.slot > current_slot.load(Ordering::Relaxed) {
                                        current_slot.store(slot_info.slot, Ordering::Relaxed);
                                        CURRENT_SLOT.set(slot_info.slot as i64);
                                        let _ = slot_sender.send(slot_info.slot);
                                    }
                                }
                            }
                            Err(_) => {
                                // timedout reconnect to pubsub
                                warn!("slot pub sub disconnected reconnecting");
                                break;
                            }
                        }
                    }
                    unsub();
                } else if let Err(e) = res {
                    error!("could not subsribe to the slot {}", e);
                }
            }
        });

        let estimated_slot = self.estimated_slot.clone();
        let current_slot = self.current_slot.clone();
        let this = self.clone();
        let estimated_slot_calculation = tokio::spawn(async move {
            // this is an estimated slot. we get the current slot and if we do not recieve any notification in 400ms we update it manually
            let mut slot_reciever = slot_reciever;
            loop {
                let update_connections = match tokio::time::timeout(
                    Duration::from_millis(AVERAGE_SLOT_CHANGE_TIME_IN_MILLIS),
                    slot_reciever.recv(),
                )
                .await
                {
                    Ok(recv) => {
                        if let Some(slot) = recv {
                            if slot > estimated_slot.load(Ordering::Relaxed) {
                                // incase of multilple slot update events / take the current slot
                                let current_slot = current_slot.load(Ordering::Relaxed);
                                estimated_slot.store(current_slot, Ordering::Relaxed);
                                ESTIMATED_SLOT.set(current_slot as i64);
                                true
                            } else {
                                // queue is late estimate slot is already ahead
                                false
                            }
                        } else {
                            false
                        }
                    }
                    Err(_) => {
                        let slot = estimated_slot.fetch_add(1, Ordering::Relaxed);
                        ESTIMATED_SLOT.set((slot + 1) as i64);
                        true
                    }
                };

                if update_connections {
                    this.update_quic_connections().await;
                }
            }
        });

        Ok(vec![
            jh_update_leaders,
            slot_sub_task,
            estimated_slot_calculation,
        ])
    }

    pub fn get_estimated_slot(&self) -> u64 {
        self.estimated_slot.load(Ordering::Relaxed)
    }
}
