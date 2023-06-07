use std::{
    sync::Arc,
    collections::VecDeque, str::FromStr,
};

use dashmap::DashMap;
use log::warn;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::response::RpcContactInfo;
use solana_sdk::{slot_history::Slot, pubkey::Pubkey};
use tokio::sync::RwLock;


pub struct LeaderData {
    pub contact_info: Arc<RpcContactInfo>,
    pub leader_slot: Slot,
}
pub struct LeaderSchedule {
    leader_schedule: RwLock<VecDeque<LeaderData>>,
    leaders_to_cache_count: usize,
    cluster_nodes: Arc<DashMap<Pubkey, Arc<RpcContactInfo>>>,
}

impl LeaderSchedule {
    pub fn new(leaders_to_cache_count: usize) -> Self {
        Self { 
            leader_schedule: RwLock::new(VecDeque::new()), 
            leaders_to_cache_count,
            cluster_nodes: Arc::new(DashMap::new()),
         }
    }

    pub async fn len(&self) -> usize {
        self.leader_schedule.read().await.len()
    }

    pub fn cluster_nodes_len(&self) -> usize {
        self.cluster_nodes.len()
    }

    pub async fn load_cluster_info(&self, rpc_client: Arc<RpcClient>) -> anyhow::Result<()> {
        let cluster_nodes = rpc_client.get_cluster_nodes().await?;
        cluster_nodes.iter().for_each(|x| {
            if let Ok(pubkey) = Pubkey::from_str(x.pubkey.as_str()) {
                self.cluster_nodes.insert(pubkey, Arc::new(x.clone()));
            }
        });
        Ok(())
    }

    pub async fn update_leader_schedule(&self, rpc_client: Arc<RpcClient>, current_slot: u64, estimated_slot: u64) -> anyhow::Result<()> {
        let (queue_begin_slot, queue_end_slot) = {
            let mut leader_queue = self.leader_schedule.write().await;
            // remove old leaders
            while leader_queue.front().map_or(current_slot, |x| x.leader_slot) < current_slot {
                leader_queue.pop_front();
            }

            let last_element = leader_queue
                .back()
                .map_or(estimated_slot, |x| x.leader_slot);
            (estimated_slot, last_element)
        };

        let last_slot_needed = queue_begin_slot + self.leaders_to_cache_count as u64;

        if last_slot_needed > queue_end_slot + 1 {
            let first_slot_to_fetch = queue_end_slot + 1;
            let leaders = rpc_client
                .get_slot_leaders(first_slot_to_fetch, last_slot_needed - first_slot_to_fetch)
                .await?;

            let mut leader_queue = self.leader_schedule.write().await;
            for i in first_slot_to_fetch..last_slot_needed {
                let current_leader = (i - first_slot_to_fetch) as usize;
                let leader = leaders[current_leader];
                if !self.cluster_nodes.contains_key(&leader) {
                    self.load_cluster_info(rpc_client.clone()).await?;
                }

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
        }
        Ok(())
    }

    pub async fn get_leaders(&self, from_slot: u64, to_slot: u64) -> Vec<Arc<RpcContactInfo>> {
        let leader_schedule = self.leader_schedule.read().await;
        let mut next_leaders = vec![];
        for leader in leader_schedule.iter() {
            if leader.leader_slot >= from_slot && leader.leader_slot <= to_slot {
                next_leaders.push(leader.contact_info.clone());
            } else if leader.leader_slot > to_slot {
                break;
            }
        }
        next_leaders
    }
}