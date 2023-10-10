use crate::leader_schedule::LeaderScheduleEvent;
use serde::{Deserialize, Serialize};
use solana_lite_rpc_core::stores::data_cache::DataCache;

#[derive(Debug, Default, Copy, Clone, PartialOrd, PartialEq, Eq, Ord, Serialize, Deserialize)]
pub struct ScheduleEpochData {
    pub current_epoch: u64,
    pub slots_in_epoch: u64,
    pub last_slot_in_epoch: u64,
    pub current_confirmed_slot: u64,
    pub new_rate_activation_epoch: Option<solana_sdk::clock::Epoch>,
}

impl ScheduleEpochData {
    pub async fn process_new_confirmed_slot(
        &mut self,
        new_slot: u64,
        data_cache: &DataCache,
    ) -> Option<LeaderScheduleEvent> {
        if self.current_confirmed_slot < new_slot {
            self.current_confirmed_slot = new_slot;
            log::trace!("Receive slot slot: {new_slot:?}");
            self.manage_change_epoch(data_cache).await
        } else {
            None
        }
    }

    async fn manage_change_epoch(&mut self, data_cache: &DataCache) -> Option<LeaderScheduleEvent> {
        //execute leaderschedule calculus at the last slot of the current epoch.
        //account change of the slot has been send at confirmed slot.
        //first epoch slot send all stake change and during this send no slot is send.
        //to avoid to delay too much the schedule, start the calculus at the end of the epoch.
        //the first epoch slot arrive very late cause of the stake account notification from the validator.
        if self.current_confirmed_slot >= self.last_slot_in_epoch {
            log::info!("Change epoch at slot:{}", self.current_confirmed_slot);
            let next_epoch = data_cache
                .epoch_data
                .get_epoch_at_slot(self.last_slot_in_epoch + 1);
            self.current_epoch = next_epoch.epoch;
            self.last_slot_in_epoch = data_cache
                .epoch_data
                .get_last_slot_in_epoch(next_epoch.epoch);

            //start leader schedule calculus
            //at current epoch change the schedule is calculated for the next epoch.
            Some(crate::leader_schedule::LeaderScheduleEvent::Init(
                self.current_epoch,
                self.slots_in_epoch,
                self.new_rate_activation_epoch.clone(),
            ))
        } else {
            None
        }
    }
}
