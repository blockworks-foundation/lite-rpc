use crate::stake::{StakeMap, StakeStore};
use crate::utils::{Takable, TakeResult};
use crate::vote::EpochVoteStakes;
use crate::vote::EpochVoteStakesCache;
use crate::vote::StoredVote;
use crate::vote::{VoteMap, VoteStore};
use futures::future::join_all;
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
    pub schedule: LeaderSchedule,
    pub epoch: u64,
}

impl LeaderScheduleGeneratedData {
    pub fn get_schedule_by_nodes(schedule: &LeaderSchedule) -> HashMap<String, Vec<usize>> {
        schedule
            .get_slot_leaders()
            .iter()
            .enumerate()
            .map(|(i, pk)| (pk.to_string(), i))
            .into_group_map()
            .into_iter()
            .collect()
    }
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

#[allow(clippy::large_enum_variant)] //256 byte large and only use during schedule calculus.
pub enum LeaderScheduleEvent {
    Init(u64, u64, Option<solana_sdk::clock::Epoch>),
    MergeStoreAndSaveSchedule(
        StakeMap,
        VoteMap,
        EpochVoteStakesCache,
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
        LeaderScheduleEvent::Init(new_epoch, slots_in_epoch, new_rate_activation_epoch) => {
            match (&mut stakestore.stakes, &mut votestore.votes).take() {
                TakeResult::Map(((stake_map, mut stake_history), (vote_map, mut epoch_cache))) => {
                    log::info!("LeaderScheduleEvent::CalculateScedule");
                    //do the calculus in a blocking task.
                    let jh = tokio::task::spawn_blocking({
                        move || {
                            let epoch_vote_stakes = calculate_epoch_stakes(
                                &stake_map,
                                &vote_map,
                                new_epoch,
                                stake_history.as_mut(),
                                new_rate_activation_epoch,
                            );

                            let next_epoch = new_epoch + 1;
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

                            epoch_cache.add_stakes_for_epoch(EpochVoteStakes {
                                epoch: new_epoch,
                                vote_stakes: epoch_vote_stakes,
                            });

                            log::info!("End calculate leader schedule");

                            LeaderScheduleEvent::MergeStoreAndSaveSchedule(
                                stake_map,
                                vote_map,
                                epoch_cache,
                                LeaderScheduleGeneratedData {
                                    schedule: leader_schedule,
                                    epoch: next_epoch,
                                },
                                (new_epoch, slots_in_epoch, new_rate_activation_epoch),
                                stake_history,
                            )
                        }
                    });
                    LeaderScheduleResult::TaskHandle(jh)
                }
                TakeResult::Taken(stake_notify) => {
                    let notif_jh = tokio::spawn({
                        async move {
                            let notifs = stake_notify
                                .iter()
                                .map(|n| n.notified())
                                .collect::<Vec<tokio::sync::futures::Notified>>();
                            join_all(notifs).await;
                            LeaderScheduleEvent::Init(
                                new_epoch,
                                slots_in_epoch,
                                new_rate_activation_epoch,
                            )
                        }
                    });
                    LeaderScheduleResult::TaskHandle(notif_jh)
                }
            }
        }
        LeaderScheduleEvent::MergeStoreAndSaveSchedule(
            stake_map,
            vote_map,
            epoch_cache,
            schedule_data,
            (new_epoch, slots_in_epoch, epoch_schedule),
            stake_history,
        ) => {
            log::info!("LeaderScheduleEvent::MergeStoreAndSaveSchedule RECV");
            match (
                stakestore.stakes.merge((stake_map, stake_history)),
                votestore.votes.merge((vote_map, epoch_cache)),
            ) {
                (Ok(()), Ok(())) => LeaderScheduleResult::End(schedule_data),
                _ => {
                    //this shoud never arrive because the store has been extracted before.
                    //TODO remove this error using type state
                    log::warn!("LeaderScheduleEvent::MergeStoreAndSaveSchedule merge stake or vote fail, -restart Schedule");
                    LeaderScheduleResult::Event(LeaderScheduleEvent::Init(
                        new_epoch,
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
    new_epoch: u64,
    mut stake_history: Option<&mut StakeHistory>,
    new_rate_activation_epoch: Option<solana_sdk::clock::Epoch>,
) -> HashMap<Pubkey, (u64, Arc<StoredVote>)> {
    //code taken from Solana code: runtime::stakes::activate_epoch function
    //update stake history with current end epoch stake values.
    //stake history is added for the ended epoch using all stakes at the end of the epoch.
    let ended_epoch = new_epoch - 1;
    let stake_history_entry =
        stake_map
            .values()
            .fold(StakeActivationStatus::default(), |acc, stake_account| {
                let delegation = stake_account.stake;
                acc + delegation.stake_activating_and_deactivating(
                    ended_epoch,
                    stake_history.as_deref(),
                    new_rate_activation_epoch,
                )
            });
    match stake_history {
        Some(ref mut stake_history) => stake_history.add(ended_epoch, stake_history_entry),
        None => log::warn!("Vote stake calculus without Stake History"),
    };

    //calculate schedule stakes at beginning of new epoch.
    //Next epoch schedule use the stake at the beginning of last epoch.
    let delegated_stakes: HashMap<Pubkey, u64> =
        stake_map
            .values()
            .fold(HashMap::default(), |mut delegated_stakes, stake_account| {
                let delegation = stake_account.stake;
                let entry = delegated_stakes.entry(delegation.voter_pubkey).or_default();
                *entry += delegation.stake(
                    new_epoch,
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
) -> LeaderSchedule {
    let stakes_map: HashMap<Pubkey, u64> = stake_vote_map
        .iter()
        .filter_map(|(_, (stake, vote_account))| {
            (*stake != 0u64).then_some((vote_account.vote_data.node_pubkey, *stake))
        })
        .into_grouping_map()
        .aggregate(|acc, _node_pubkey, stake| Some(acc.unwrap_or_default() + stake));
    let mut stakes: Vec<(Pubkey, u64)> = stakes_map
        .into_iter()
        .map(|(key, stake)| (key, stake))
        .collect();

    let mut seed = [0u8; 32];
    seed[0..8].copy_from_slice(&epoch.to_le_bytes());
    sort_stakes(&mut stakes);
    log::info!("calculate_leader_schedule stakes:{stakes:?} epoch:{epoch}");
    LeaderSchedule::new(&stakes, seed, slots_in_epoch, NUM_CONSECUTIVE_LEADER_SLOTS)
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
