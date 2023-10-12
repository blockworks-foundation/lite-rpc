use crate::stores::block_information_store::BlockInformation;
use crate::stores::data_cache::DataCache;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;

#[derive(Clone, Default)]
pub struct CalculatedSchedule {
    pub current: Option<LeaderScheduleData>,
    pub next: Option<LeaderScheduleData>,
}

impl CalculatedSchedule {
    pub async fn get_leader_schedule_for_slot(
        &self,
        slot: Option<u64>,
        commitment: Option<CommitmentConfig>,
        data_cache: &DataCache,
    ) -> Option<HashMap<String, Vec<usize>>> {
        let commitment = commitment.unwrap_or_else(|| CommitmentConfig::confirmed());
        let slot = match slot {
            Some(slot) => slot,
            None => {
                let BlockInformation { slot, .. } = data_cache
                    .block_information_store
                    .get_latest_block(commitment)
                    .await;
                slot
            }
        };
        let epoch = data_cache.epoch_data.get_epoch_at_slot(slot);

        let get_schedule = |schedule_data: Option<&LeaderScheduleData>| {
            schedule_data.and_then(|current| {
                (current.epoch == epoch.epoch).then_some(current.schedule.clone())
            })
        };
        get_schedule(self.current.as_ref()).or_else(|| get_schedule(self.next.as_ref()))
    }
}

#[derive(Clone)]
pub struct LeaderScheduleData {
    pub schedule: HashMap<String, Vec<usize>>,
    pub epoch: u64,
}
