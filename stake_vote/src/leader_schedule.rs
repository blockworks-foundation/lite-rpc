use crate::stake::StakeMap;
use crate::stake::StakeStore;
use crate::vote::VoteMap;
use crate::vote::VoteStore;
use futures::stream::FuturesUnordered;
use solana_lite_rpc_core::structures::leaderschedule::LeaderScheduleData;
use solana_program::sysvar::epoch_schedule::EpochSchedule;
use solana_sdk::stake_history::StakeHistory;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub enum LeaderScheduleEvent {
    Init(u64, u64, Option<solana_sdk::clock::Epoch>),
    MergeStoreAndSaveSchedule(
        StakeMap,
        VoteMap,
        LeaderScheduleData,
        (u64, u64, Arc<EpochSchedule>),
        Option<StakeHistory>,
    ),
}

//Execute the leader schedule process.
pub fn run_leader_schedule_events(
    event: LeaderScheduleEvent,
    bootstrap_tasks: &mut FuturesUnordered<JoinHandle<LeaderScheduleEvent>>,
    stakestore: &mut StakeStore,
    votestore: &mut VoteStore,
) -> Option<LeaderScheduleData> {
    todo!();
}
