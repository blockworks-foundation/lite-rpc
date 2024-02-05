use crate::leader_schedule::LeaderScheduleEvent;
use serde::{Deserialize, Serialize};
use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_sdk::stake_history::StakeHistory;

//#[derive(Debug, Default, Copy, Clone, PartialOrd, PartialEq, Eq, Ord, Serialize, Deserialize)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ScheduleEpochData {
    pub current_epoch: u64,
    pub slots_in_epoch: u64,
    pub last_slot_in_epoch: u64,
    pub current_confirmed_slot: u64,
    pub new_rate_activation_epoch: Option<solana_sdk::clock::Epoch>,
    //to start  a new epoch and schedule, the new stake blockstore
    //Must be notified  and the end  epoch slot notfied.
    //these field store each event.
    //If they're defined  an new epoch and  leader schedule can append.
    new_stake_history: Option<StakeHistory>,
    next_epoch_change: Option<(u64, u64)>,
}

impl ScheduleEpochData {
    pub fn new(
        current_epoch: u64,
        slots_in_epoch: u64,
        last_slot_in_epoch: u64,
        current_confirmed_slot: u64,
        new_rate_activation_epoch: Option<solana_sdk::clock::Epoch>,
    ) -> Self {
        ScheduleEpochData {
            current_epoch,
            slots_in_epoch,
            last_slot_in_epoch,
            current_confirmed_slot,
            new_rate_activation_epoch,
            new_stake_history: None,
            next_epoch_change: None,
        }
    }

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

    pub fn set_epoch_stake_history(
        &mut self,
        history: StakeHistory,
    ) -> Option<LeaderScheduleEvent> {
        log::debug!("set_epoch_stake_history");
        self.new_stake_history = Some(history);
        self.verify_epoch_change()
    }

    async fn manage_change_epoch(&mut self, data_cache: &DataCache) -> Option<LeaderScheduleEvent> {
        //execute leaderschedule calculus at the last slot of the current epoch.
        //account change of the slot has been send at confirmed slot.
        //first epoch slot send all stake change and during this send no slot is send.
        //to avoid to delay too much the schedule, start the calculus at the end of the epoch.
        //the first epoch slot arrive very late cause of the stake account notification from the validator.
        if self.current_confirmed_slot >= self.last_slot_in_epoch {
            log::debug!(
                "manage_change_epoch at slot:{} last_slot_in_epoch:{}",
                self.current_confirmed_slot,
                self.last_slot_in_epoch
            );
            let next_epoch = data_cache
                .epoch_data
                .get_epoch_at_slot(self.last_slot_in_epoch + 1);
            let last_slot_in_epoch = data_cache
                .epoch_data
                .get_last_slot_in_epoch(next_epoch.epoch);

            //start leader schedule calculus
            //at current epoch change the schedule is calculated for the next epoch.
            self.next_epoch_change = Some((next_epoch.epoch, last_slot_in_epoch));
            self.verify_epoch_change()
        } else {
            None
        }
    }

    fn verify_epoch_change(&mut self) -> Option<LeaderScheduleEvent> {
        if self.new_stake_history.is_some() && self.next_epoch_change.is_some() {
            log::info!("Change epoch at slot:{}", self.current_confirmed_slot);
            let (next_epoch, last_slot_in_epoch) = self.next_epoch_change.take().unwrap(); //unwrap tested before.
            self.current_epoch = next_epoch;
            self.last_slot_in_epoch = last_slot_in_epoch;

            //start leader schedule calculus
            //at current epoch change the schedule is calculated for the next epoch.
            Some(crate::leader_schedule::LeaderScheduleEvent::Init(
                self.current_epoch,
                self.slots_in_epoch,
                self.new_rate_activation_epoch,
                self.new_stake_history.take().unwrap(), //unwrap tested before
            ))
        } else {
            None
        }
    }
}
