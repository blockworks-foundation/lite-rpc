use anyhow::{bail, Error};
use async_trait::async_trait;
use solana_lite_rpc_core::structures::epoch::EpochCache;
use solana_lite_rpc_core::structures::leaderschedule::CalculatedSchedule;
use solana_lite_rpc_core::{
    structures::leader_data::LeaderData, traits::leaders_fetcher_interface::LeaderFetcherInterface,
};
use std::sync::Arc;
use tokio::sync::RwLock;

// Stores leaders for slots from older to newer in leader schedule
// regularly removed old leaders and adds new ones
pub struct GrpcLeaderGetter {
    epoch_data: EpochCache,
    leader_schedule: Arc<RwLock<CalculatedSchedule>>,
}

impl GrpcLeaderGetter {
    pub fn new(leader_schedule: Arc<RwLock<CalculatedSchedule>>, epoch_data: EpochCache) -> Self {
        Self {
            leader_schedule,
            epoch_data,
        }
    }
}

#[async_trait]
impl LeaderFetcherInterface for GrpcLeaderGetter {
    async fn get_slot_leaders(
        &self,
        from: solana_sdk::slot_history::Slot,
        to: solana_sdk::slot_history::Slot,
    ) -> anyhow::Result<Vec<LeaderData>> {
        //get epoch of from/to slot to see if they're in the current stored epoch.
        let from_epoch = self.epoch_data.get_epoch_at_slot(from).epoch;
        let to_epoch = self.epoch_data.get_epoch_at_slot(to).epoch;
        let leader_schedule_data = self.leader_schedule.read().await;
        let current_epoch = leader_schedule_data
            .current
            .as_ref()
            .map(|e| e.epoch)
            .unwrap_or(from_epoch);
        let next_epoch = leader_schedule_data
            .current
            .as_ref()
            .map(|e| e.epoch)
            .unwrap_or(to_epoch)
            + 1;
        if from > to {
            bail!(
                "invalid arguments for get_slot_leaders: from:{from} to:{to} from:{from} > to:{to}"
            );
        }
        if from_epoch < current_epoch || from_epoch > next_epoch {
            bail!(
                "invalid arguments for get_slot_leaders: from:{from} to:{to} \
             from_epoch:{from_epoch} < current_epoch:{current_epoch} \
             || from_epoch > next_epoch:{next_epoch}"
            );
        }
        if to_epoch < current_epoch || to_epoch > next_epoch {
            bail!(
                "invalid arguments for get_slot_leaders: from:{from} to:{to} \
             to_epoch:{to_epoch} < current_epoch:{current_epoch} \
                || to_epoch:{to_epoch} > next_epoch:{next_epoch}"
            );
        }

        let limit = to.saturating_sub(from);

        let schedule = leader_schedule_data
            .get_slot_leaders(from, limit, self.epoch_data.get_epoch_schedule())
            .await
            .map_err(Error::msg)?;

        Ok(schedule
            .into_iter()
            .enumerate()
            .map(|(index, pubkey)| LeaderData {
                leader_slot: from + index as u64,
                pubkey,
            })
            .collect())
    }
}
