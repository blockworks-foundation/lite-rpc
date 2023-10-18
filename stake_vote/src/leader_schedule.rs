use crate::stake::{StakeMap, StakeStore};
use crate::vote::StoredVote;
use crate::vote::{VoteMap, VoteStore};
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use solana_ledger::leader_schedule::LeaderSchedule;
use solana_sdk::clock::NUM_CONSECUTIVE_LEADER_SLOTS;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::state::StakeActivationStatus;
use solana_sdk::stake_history::StakeHistory;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct LeaderScheduleGeneratedData {
    pub schedule: HashMap<String, Vec<usize>>,
    pub vote_stakes: HashMap<Pubkey, (u64, Arc<StoredVote>)>,
    pub epoch: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EpochStake {
    epoch: u64,
    stake_vote_map: HashMap<Pubkey, (u64, Arc<StoredVote>)>,
}

/*
Leader schedule calculus state diagram

InitLeaderschedule
       |
   |extract store stake and vote|
     |                   |
   Error          CalculateScedule(stakes, votes)
     |                   |
 | Wait(1s)|       |Calculate schedule|
     |                       |
InitLeaderscedule        MergeStore(stakes, votes, schedule)
                             |                      |
                           Error                   SaveSchedule(schedule)
                             |                              |
              |never occurs restart (wait 1s)|         |save schedule and verify (opt)|
                             |
                      InitLeaderscedule
*/

pub enum LeaderScheduleEvent {
    Init(u64, u64, Option<solana_sdk::clock::Epoch>),
    MergeStoreAndSaveSchedule(
        StakeMap,
        VoteMap,
        LeaderScheduleGeneratedData,
        (u64, u64, Option<solana_sdk::clock::Epoch>),
        Option<StakeHistory>,
    ),
}

enum LeaderScheduleResult {
    TaskHandle(JoinHandle<LeaderScheduleEvent>),
    Event(LeaderScheduleEvent),
    End(LeaderScheduleGeneratedData),
}

//Execute the leader schedule process.
pub fn run_leader_schedule_events(
    event: LeaderScheduleEvent,
    schedule_tasks: &mut FuturesUnordered<JoinHandle<LeaderScheduleEvent>>,
    stakestore: &mut StakeStore,
    votestore: &mut VoteStore,
) -> Option<LeaderScheduleGeneratedData> {
    let result = process_leadershedule_event(event, stakestore, votestore);
    match result {
        LeaderScheduleResult::TaskHandle(jh) => {
            schedule_tasks.push(jh);
            None
        }
        LeaderScheduleResult::Event(event) => {
            run_leader_schedule_events(event, schedule_tasks, stakestore, votestore)
        }
        LeaderScheduleResult::End(schedule) => Some(schedule),
    }
}

fn process_leadershedule_event(
    //    rpc_url: String,
    event: LeaderScheduleEvent,
    stakestore: &mut StakeStore,
    votestore: &mut VoteStore,
) -> LeaderScheduleResult {
    match event {
        LeaderScheduleEvent::Init(current_epoch, slots_in_epoch, new_rate_activation_epoch) => {
            match (
                StakeStore::take_stakestore(stakestore),
                VoteStore::take_votestore(votestore),
            ) {
                (Ok((stake_map, mut stake_history)), Ok(vote_map)) => {
                    log::info!("LeaderScheduleEvent::CalculateScedule");
                    //do the calculus in a blocking task.
                    let jh = tokio::task::spawn_blocking({
                        move || {
                            let next_epoch = current_epoch + 1;
                            let epoch_vote_stakes = calculate_epoch_stakes(
                                &stake_map,
                                &vote_map,
                                current_epoch,
                                next_epoch,
                                stake_history.as_mut(),
                                new_rate_activation_epoch,
                            );

                            let leader_schedule = calculate_leader_schedule(
                                &epoch_vote_stakes,
                                next_epoch,
                                slots_in_epoch,
                            );

                            if std::path::Path::new(crate::bootstrap::NEXT_EPOCH_VOTE_STAKES_FILE)
                                .exists()
                            {
                                if let Err(err) = std::fs::rename(
                                    crate::bootstrap::NEXT_EPOCH_VOTE_STAKES_FILE,
                                    crate::bootstrap::CURRENT_EPOCH_VOTE_STAKES_FILE,
                                ) {
                                    log::error!(
                                    "Fail to rename current leader schedule on disk because :{err}"
                                );
                                }
                            }

                            //save new vote stake in a file for bootstrap.
                            if let Err(err) = crate::utils::save_schedule_vote_stakes(
                                crate::bootstrap::NEXT_EPOCH_VOTE_STAKES_FILE,
                                &epoch_vote_stakes,
                                next_epoch,
                            ) {
                                log::error!(
                                    "Error during saving the new leader schedule of epoch:{} in a file error:{err}",
                                    next_epoch
                                );
                            }

                            log::info!("End calculate leader schedule");
                            LeaderScheduleEvent::MergeStoreAndSaveSchedule(
                                stake_map,
                                vote_map,
                                LeaderScheduleGeneratedData {
                                    schedule: leader_schedule,
                                    vote_stakes: epoch_vote_stakes,
                                    epoch: next_epoch,
                                },
                                (current_epoch, slots_in_epoch, new_rate_activation_epoch),
                                stake_history,
                            )
                        }
                    });
                    LeaderScheduleResult::TaskHandle(jh)
                }
                _ => {
                    log::error!("Create leadershedule init event error during extract store");
                    LeaderScheduleResult::Event(LeaderScheduleEvent::Init(
                        current_epoch,
                        slots_in_epoch,
                        new_rate_activation_epoch,
                    ))
                }
            }
        }
        LeaderScheduleEvent::MergeStoreAndSaveSchedule(
            stake_map,
            vote_map,
            schedule_data,
            (current_epoch, slots_in_epoch, epoch_schedule),
            stake_history,
        ) => {
            log::info!("LeaderScheduleEvent::MergeStoreAndSaveSchedule RECV");
            match (
                StakeStore::merge_stakestore(stakestore, stake_map, stake_history),
                VoteStore::merge_votestore(votestore, vote_map),
            ) {
                (Ok(()), Ok(())) => LeaderScheduleResult::End(schedule_data),
                _ => {
                    //this shoud never arrive because the store has been extracted before.
                    //TODO remove this error using type state
                    log::warn!("LeaderScheduleEvent::MergeStoreAndSaveSchedule merge stake or vote fail, -restart Schedule");
                    LeaderScheduleResult::Event(LeaderScheduleEvent::Init(
                        current_epoch,
                        slots_in_epoch,
                        epoch_schedule,
                    ))
                }
            }
        }
    }
}

fn calculate_epoch_stakes(
    stake_map: &StakeMap,
    vote_map: &VoteMap,
    current_epoch: u64,
    next_epoch: u64,
    mut stake_history: Option<&mut StakeHistory>,
    new_rate_activation_epoch: Option<solana_sdk::clock::Epoch>,
) -> HashMap<Pubkey, (u64, Arc<StoredVote>)> {
    //code taken from Solana code: runtime::stakes::activate_epoch function
    //update stake history with current end epoch stake values.
    let stake_history_entry =
        stake_map
            .values()
            .fold(StakeActivationStatus::default(), |acc, stake_account| {
                let delegation = stake_account.stake;
                acc + delegation.stake_activating_and_deactivating(
                    current_epoch,
                    stake_history.as_deref(),
                    new_rate_activation_epoch,
                )
            });
    match stake_history {
        Some(ref mut stake_history) => stake_history.add(current_epoch, stake_history_entry),
        None => log::warn!("Vote stake calculus without Stake History"),
    };

    //calculate schedule stakes.
    let delegated_stakes: HashMap<Pubkey, u64> =
        stake_map
            .values()
            .fold(HashMap::default(), |mut delegated_stakes, stake_account| {
                let delegation = stake_account.stake;
                let entry = delegated_stakes.entry(delegation.voter_pubkey).or_default();
                *entry += delegation.stake(
                    next_epoch,
                    stake_history.as_deref(),
                    new_rate_activation_epoch,
                );
                delegated_stakes
            });

    let staked_vote_map: HashMap<Pubkey, (u64, Arc<StoredVote>)> = vote_map
        .values()
        .map(|vote_account| {
            let delegated_stake = delegated_stakes
                .get(&vote_account.pubkey)
                .copied()
                .unwrap_or_else(|| {
                    log::info!(
                        "calculate_epoch_stakes stake with no vote account:{}",
                        vote_account.pubkey
                    );
                    Default::default()
                });
            (vote_account.pubkey, (delegated_stake, vote_account.clone()))
        })
        .collect();
    staked_vote_map
}

//Copied from leader_schedule_utils.rs
// Mostly cribbed from leader_schedule_utils
pub fn calculate_leader_schedule(
    stake_vote_map: &HashMap<Pubkey, (u64, Arc<StoredVote>)>,
    epoch: u64,
    slots_in_epoch: u64,
) -> HashMap<String, Vec<usize>> {
    let mut stakes: Vec<(Pubkey, u64)> = stake_vote_map
        .iter()
        .filter_map(|(_, (stake, vote_account))| {
            (*stake > 0).then_some((vote_account.vote_data.node_pubkey, *stake))
        })
        .collect();

    let mut seed = [0u8; 32];
    seed[0..8].copy_from_slice(&epoch.to_le_bytes());
    sort_stakes(&mut stakes);
    log::info!("calculate_leader_schedule stakes:{stakes:?} epoch:{epoch}");
    let schedule = LeaderSchedule::new(&stakes, seed, slots_in_epoch, NUM_CONSECUTIVE_LEADER_SLOTS);

    let slot_schedule = schedule
        .get_slot_leaders()
        .iter()
        .enumerate()
        .map(|(i, pk)| (pk.to_string(), i))
        .into_group_map()
        .into_iter()
        .collect();
    slot_schedule
}

// Cribbed from leader_schedule_utils
fn sort_stakes(stakes: &mut Vec<(Pubkey, u64)>) {
    // Sort first by stake. If stakes are the same, sort by pubkey to ensure a
    // deterministic result.
    // Note: Use unstable sort, because we dedup right after to remove the equal elements.
    stakes.sort_unstable_by(|(l_pubkey, l_stake), (r_pubkey, r_stake)| {
        if r_stake == l_stake {
            r_pubkey.cmp(l_pubkey)
        } else {
            r_stake.cmp(l_stake)
        }
    });

    // Now that it's sorted, we can do an O(n) dedup.
    stakes.dedup();
}
