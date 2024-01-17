use anyhow::{bail, Context};
use async_trait::async_trait;
use itertools::Itertools;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_cluster::{leader_data::LeaderData, leaders_fetcher_interface::LeaderFetcherInterface};
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::RwLock;

// Stores leaders for slots from older to newer in leader schedule
// regularly removed old leaders and adds new ones
pub struct JsonRpcLeaderGetter {
    rpc_client: Arc<RpcClient>,
    leader_schedule: RwLock<VecDeque<LeaderData>>,
    leaders_to_cache_count: u64,
    old_slot_leader_cache_size: u64,
}

impl JsonRpcLeaderGetter {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        leaders_to_cache_count: u64,
        old_slot_leader_cache_size: u64,
    ) -> Self {
        Self {
            rpc_client,
            leader_schedule: RwLock::new(VecDeque::new()),
            leaders_to_cache_count,
            old_slot_leader_cache_size,
        }
    }

    pub async fn update_leader_schedule(&self, slot: u64) -> anyhow::Result<()> {
        // remove old elements
        let mut leader_queue = self.leader_schedule.write().await;
        // remove old leaders
        while leader_queue.front().map_or(slot, |x| x.leader_slot)
            < slot.saturating_sub(self.old_slot_leader_cache_size)
        {
            leader_queue.pop_front();
        }

        let last_slot_needed = slot + self.leaders_to_cache_count;
        let first_slot_to_fetch = leader_queue.back().map_or(slot, |x| x.leader_slot + 1);

        if last_slot_needed >= first_slot_to_fetch {
            let leaders = self
                .rpc_client
                .get_slot_leaders(first_slot_to_fetch, last_slot_needed - first_slot_to_fetch)
                .await
                .context("failed to get slot leaders")?;

            for leader_slot in first_slot_to_fetch..last_slot_needed {
                let current_leader = (leader_slot - first_slot_to_fetch) as usize;
                let pubkey = leaders[current_leader];
                leader_queue.push_back(LeaderData {
                    leader_slot,
                    pubkey,
                });
            }
        }
        Ok(())
    }
}

#[async_trait]
impl LeaderFetcherInterface for JsonRpcLeaderGetter {
    async fn get_slot_leaders(
        &self,
        from: solana_sdk::slot_history::Slot,
        to: solana_sdk::slot_history::Slot,
    ) -> anyhow::Result<Vec<LeaderData>> {
        if to <= from || to - from > self.leaders_to_cache_count {
            bail!("invalid arguments for get_slot_leaders");
        }
        let schedule = self.leader_schedule.read().await;

        let schedule = if schedule.is_empty()
            || schedule.front().unwrap().leader_slot > from
            || schedule.back().unwrap().leader_slot < to
        {
            // rebuilding the leader schedule
            drop(schedule);
            self.update_leader_schedule(from).await?;
            self.leader_schedule.read().await
        } else {
            schedule
        };
        Ok(schedule
            .iter()
            .filter(|x| x.leader_slot >= from && x.leader_slot <= to)
            .cloned()
            .collect_vec())
    }
}
