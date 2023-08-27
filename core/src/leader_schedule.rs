use crate::{cluster_info::ClusterInfo, traits::slot_leaders_getter::SlotLeadersGetter};
use anyhow::Context;
use log::warn;
use solana_rpc_client_api::response::RpcContactInfo;
use solana_sdk::slot_history::Slot;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::RwLock;

pub struct LeaderData {
    pub contact_info: Arc<RpcContactInfo>,
    pub leader_slot: Slot,
}
pub struct LeaderSchedule {
    leader_schedule: RwLock<VecDeque<LeaderData>>,
    leaders_to_cache_count: usize,
    cluster_info: ClusterInfo,
    slot_leaders_getter: Arc<dyn SlotLeadersGetter>,
}

impl LeaderSchedule {
    pub fn new(
        leaders_to_cache_count: usize,
        slot_leader_getter: Arc<dyn SlotLeadersGetter>,
        cluster_info: ClusterInfo,
    ) -> Self {
        Self {
            leader_schedule: RwLock::new(VecDeque::new()),
            leaders_to_cache_count,
            cluster_info,
            slot_leaders_getter: slot_leader_getter,
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub async fn len(&self) -> usize {
        self.leader_schedule.read().await.len()
    }

    pub async fn update_leader_schedule(
        &self,
        current_slot: u64,
        estimated_slot: u64,
    ) -> anyhow::Result<()> {
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

            let leaders = self
                .slot_leaders_getter
                .get_slot_leaders(first_slot_to_fetch, last_slot_needed)
                .await
                .context("failed to get slot leaders")?;

            let mut leader_queue = self.leader_schedule.write().await;
            for i in first_slot_to_fetch..last_slot_needed {
                let current_leader = (i - first_slot_to_fetch) as usize;
                let leader = leaders[current_leader];
                if !self.cluster_info.cluster_nodes.contains_key(&leader) {
                    warn!("Cluster info missing for {}", leader);
                }

                match self.cluster_info.cluster_nodes.get(&leader) {
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
