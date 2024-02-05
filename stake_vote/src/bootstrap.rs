use crate::epoch::ScheduleEpochData;
use crate::leader_schedule::LeaderScheduleGeneratedData;
use crate::stake::StakeMap;
use crate::stake::StakeStore;
use crate::utils::{Takable, TakeResult};
use crate::vote::EpochVoteStakes;
use crate::vote::EpochVoteStakesCache;
use crate::vote::VoteMap;
use crate::vote::VoteStore;
use anyhow::bail;
use futures::future::join_all;
use futures_util::stream::FuturesUnordered;
use solana_client::client_error::ClientError;
use solana_client::client_error::ClientErrorKind;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_response::RpcVoteAccountStatus;
use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_lite_rpc_core::structures::leaderschedule::CalculatedSchedule;
use solana_lite_rpc_core::structures::leaderschedule::LeaderScheduleData;
use solana_program::slot_history::Slot;
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::sysvar::epoch_schedule::EpochSchedule;
use std::collections::HashMap;
use std::time::Duration;
use tokio::task::JoinHandle;

//File where the Vote and stake use to calculate the leader schedule at epoch are stored.
//Use to bootstrap current and next epoch leader schedule.
//TODO to be removed with inter RPC bootstrap and snapshot read.
pub const CURRENT_EPOCH_VOTE_STAKES_FILE: &str = "current_vote_stakes.json";
pub const NEXT_EPOCH_VOTE_STAKES_FILE: &str = "next_vote_stakes.json";

pub async fn bootstrap_schedule_epoch_data(data_cache: &DataCache) -> ScheduleEpochData {
    let new_rate_activation_epoch = solana_sdk::feature_set::FeatureSet::default()
        .new_warmup_cooldown_rate_epoch(data_cache.epoch_data.get_epoch_schedule());

    let bootstrap_epoch = crate::utils::get_current_epoch(data_cache).await;
    ScheduleEpochData::new(
        bootstrap_epoch.epoch,
        bootstrap_epoch.slots_in_epoch,
        data_cache
            .epoch_data
            .get_last_slot_in_epoch(bootstrap_epoch.epoch),
        bootstrap_epoch.absolute_slot,
        new_rate_activation_epoch,
    )
}

// Return the current and next epoxh leader schedule and the current epoch stakes of vote accounts
// if the corresponding files exist.
pub fn bootstrap_leaderschedule_from_files(
    current_epoch_of_loading: u64,
    slots_in_epoch: u64,
) -> Option<(CalculatedSchedule, RpcVoteAccountStatus)> {
    bootstrap_current_leader_schedule(slots_in_epoch, current_epoch_of_loading)
        .map(|(leader_schedule, current_epoch_stakes, _)| {
            let vote_acccounts = crate::vote::get_rpc_vote_account_info_from_current_epoch_stakes(
                &current_epoch_stakes,
            );
            (leader_schedule, vote_acccounts)
        })
        .ok()
}

// Return the current or next epoch leader schedule using the RPC calls.
pub fn bootstrap_leaderschedule_from_rpc(
    rpc_url: String,
    epoch_schedule: &EpochSchedule,
) -> Result<CalculatedSchedule, ClientError> {
    let current_epoch = get_rpc_epoch_info(rpc_url.clone())?;
    let current_schedule_by_node =
        get_rpc_leader_schedule(rpc_url.clone(), None)?.ok_or(ClientError {
            request: None,
            kind: ClientErrorKind::Custom("RPC return no leader schedule".to_string()),
        })?;

    //Calculate the slot leaders by from the node schedule because RPC call get_slot_leaders is limited to 5000 slots.
    let current_schedule_by_slot =
        crate::leader_schedule::calculate_slot_leaders_from_schedule(&current_schedule_by_node)
            .map_err(|err| ClientError {
                request: None,
                kind: ClientErrorKind::Custom(format!(
                    "Leader schedule from RPC can't generate slot leaders because:{err}"
                )),
            })?;

    //get next epoch rpc schedule
    let next_epoch = current_epoch.epoch + 1;
    let next_first_epoch_slot = epoch_schedule.get_first_slot_in_epoch(next_epoch);
    let next_schedule_by_node =
        get_rpc_leader_schedule(rpc_url.clone(), Some(next_first_epoch_slot))?.ok_or(
            ClientError {
                request: None,
                kind: ClientErrorKind::Custom("RPC return no leader schedule".to_string()),
            },
        )?;

    //Calculate the slot leaders by from the node schedule because RPC call get_slot_leaders is limited to 5000 slots.
    let next_schedule_by_slot =
        crate::leader_schedule::calculate_slot_leaders_from_schedule(&next_schedule_by_node)
            .map_err(|err| ClientError {
                request: None,
                kind: ClientErrorKind::Custom(format!(
                    "Leader schedule from RPC can't generate slot leaders because:{err}"
                )),
            })?;

    Ok(CalculatedSchedule {
        current: Some(LeaderScheduleData {
            schedule_by_node: current_schedule_by_node.clone(),
            schedule_by_slot: current_schedule_by_slot.clone(),
            epoch: current_epoch.epoch,
        }),
        next: Some(LeaderScheduleData {
            schedule_by_node: next_schedule_by_node,
            schedule_by_slot: next_schedule_by_slot,
            epoch: current_epoch.epoch + 1,
        }),
    })
}

/*
Bootstrap state changes

  InitBootstrap
       |
  |Fetch accounts|

    |          |
  Error   BootstrapAccountsFetched(account list)
    |          |
 |Exit|     |Extract stores|
               |         |
            Error     StoreExtracted(account list, stores)
               |                       |
            | Wait(1s)|           |Merge accounts in store|
               |                            |          |
   BootstrapAccountsFetched(account list)  Error    AccountsMerged(stores)
                                            |                        |
                                      |Log and skip|          |Merges store|
                                      |Account     |            |         |
                                                               Error     End
                                                                |
                                                              |never occurs restart|
                                                                 |
                                                          InitBootstrap
*/

pub fn run_bootstrap_events(
    event: BootstrapEvent,
    bootstrap_tasks: &mut FuturesUnordered<JoinHandle<BootstrapEvent>>,
    stakestore: &mut StakeStore,
    votestore: &mut VoteStore,
    slots_in_epoch: u64,
    current_epoch_of_loading: u64,
) -> anyhow::Result<Option<anyhow::Result<(CalculatedSchedule, RpcVoteAccountStatus)>>> {
    let result = process_bootstrap_event(
        event,
        stakestore,
        votestore,
        slots_in_epoch,
        current_epoch_of_loading,
    );
    match result {
        BootsrapProcessResult::TaskHandle(jh) => {
            bootstrap_tasks.push(jh);
            Ok(None)
        }
        BootsrapProcessResult::Event(event) => run_bootstrap_events(
            event,
            bootstrap_tasks,
            stakestore,
            votestore,
            slots_in_epoch,
            current_epoch_of_loading,
        ),
        BootsrapProcessResult::End(leader_schedule_result) => Ok(Some(leader_schedule_result)),
        BootsrapProcessResult::Error(err) => bail!(err),
    }
}

pub enum BootstrapEvent {
    InitBootstrap {
        sleep_time: u64,
        rpc_url: String,
    },
    BootstrapAccountsFetched(
        Vec<(Pubkey, Account)>,
        Vec<(Pubkey, Account)>,
        Account,
        String,
    ),
    StoreExtracted(
        StakeMap,
        VoteMap,
        EpochVoteStakesCache,
        Vec<(Pubkey, Account)>,
        Vec<(Pubkey, Account)>,
        Account,
        String,
    ),
    AccountsMerged(
        StakeMap,
        VoteMap,
        EpochVoteStakesCache,
        String,
        anyhow::Result<(CalculatedSchedule, RpcVoteAccountStatus)>,
    ),
    Exit,
}

#[allow(clippy::large_enum_variant)] //214 byte large and only use during bootstrap.
enum BootsrapProcessResult {
    TaskHandle(JoinHandle<BootstrapEvent>),
    Event(BootstrapEvent),
    Error(String),
    End(anyhow::Result<(CalculatedSchedule, RpcVoteAccountStatus)>),
}

fn process_bootstrap_event(
    event: BootstrapEvent,
    stakestore: &mut StakeStore,
    votestore: &mut VoteStore,
    slots_in_epoch: u64,
    current_epoch_of_loading: u64,
) -> BootsrapProcessResult {
    match event {
        BootstrapEvent::InitBootstrap {
            sleep_time,
            rpc_url,
        } => {
            let jh = tokio::task::spawn_blocking(move || {
                if sleep_time > 0 {
                    std::thread::sleep(Duration::from_secs(sleep_time));
                }
                match bootstrap_accounts(rpc_url.clone()) {
                    Ok((stakes, votes, history)) => {
                        BootstrapEvent::BootstrapAccountsFetched(stakes, votes, history, rpc_url)
                    }
                    Err(err) => {
                        log::warn!(
                            "Bootstrap account error during fetching accounts err:{err}. Exit"
                        );
                        BootstrapEvent::Exit
                    }
                }
            });
            BootsrapProcessResult::TaskHandle(jh)
        }
        BootstrapEvent::BootstrapAccountsFetched(stakes, votes, history, rpc_url) => {
            match (&mut stakestore.stakes, &mut votestore.votes).take() {
                TakeResult::Map((stake_map, (vote_map, epoch_cache))) => {
                    BootsrapProcessResult::Event(BootstrapEvent::StoreExtracted(
                        stake_map,
                        vote_map,
                        epoch_cache,
                        stakes,
                        votes,
                        history,
                        rpc_url,
                    ))
                }
                TakeResult::Taken(stake_notify) => {
                    let notif_jh = tokio::spawn({
                        async move {
                            let notifs = stake_notify
                                .iter()
                                .map(|n| n.notified())
                                .collect::<Vec<tokio::sync::futures::Notified>>();
                            join_all(notifs).await;
                            BootstrapEvent::BootstrapAccountsFetched(
                                stakes, votes, history, rpc_url,
                            )
                        }
                    });
                    BootsrapProcessResult::TaskHandle(notif_jh)
                }
            }
        }

        BootstrapEvent::StoreExtracted(
            mut stake_map,
            mut vote_map,
            mut epoch_cache,
            stakes,
            votes,
            history,
            rpc_url,
        ) => {
            let stake_history = crate::account::read_historystake_from_account(&history.data);
            if stake_history.is_none() {
                return BootsrapProcessResult::Error(
                    "Bootstrap error, can't read stake blockstore from account data.".to_string(),
                );
            }

            //merge new PA with stake map and vote map in a specific task
            let jh = tokio::task::spawn_blocking({
                move || {
                    //update pa_list to set slot update to start epoq one.
                    crate::stake::merge_program_account_in_strake_map(
                        &mut stake_map,
                        stakes,
                        0, //with RPC no way to know the slot of the account update. Set to 0.
                    );
                    crate::vote::merge_program_account_in_vote_map(
                        &mut vote_map,
                        votes,
                        0, //with RPC no way to know the slot of the account update. Set to 0.
                    );

                    match bootstrap_current_leader_schedule(
                        current_epoch_of_loading,
                        slots_in_epoch,
                    ) {
                        Ok((leader_schedule, current_epoch_stakes, next_epoch_stakes)) => {
                            let vote_acccounts =
                                crate::vote::get_rpc_vote_account_info_from_current_epoch_stakes(
                                    &current_epoch_stakes,
                                );
                            epoch_cache.add_stakes_for_epoch(current_epoch_stakes);
                            epoch_cache.add_stakes_for_epoch(next_epoch_stakes);
                            BootstrapEvent::AccountsMerged(
                                stake_map,
                                vote_map,
                                epoch_cache,
                                rpc_url,
                                Ok((leader_schedule, vote_acccounts)),
                            )
                        }
                        Err(err) => BootstrapEvent::AccountsMerged(
                            stake_map,
                            vote_map,
                            epoch_cache,
                            rpc_url,
                            Err(err),
                        ),
                    }
                }
            });
            BootsrapProcessResult::TaskHandle(jh)
        }
        BootstrapEvent::AccountsMerged(
            stake_map,
            vote_map,
            epoch_cache,
            rpc_url,
            leader_schedule_result,
        ) => {
            match (
                stakestore.stakes.merge(stake_map),
                votestore.votes.merge((vote_map, epoch_cache)),
            ) {
                (Ok(()), Ok(())) => BootsrapProcessResult::End(leader_schedule_result),
                _ => {
                    //TODO remove this error using type state
                    log::warn!("BootstrapEvent::AccountsMerged merge stake or vote fail,  non extracted stake/vote map err, restart bootstrap");
                    BootsrapProcessResult::Event(BootstrapEvent::InitBootstrap {
                        sleep_time: 10,
                        rpc_url,
                    })
                }
            }
        }
        BootstrapEvent::Exit => panic!("Bootstrap account can't be done exit"),
    }
}

#[allow(clippy::type_complexity)]
fn bootstrap_accounts(
    rpc_url: String,
) -> Result<(Vec<(Pubkey, Account)>, Vec<(Pubkey, Account)>, Account), ClientError> {
    get_stake_account(rpc_url)
        .and_then(|(stakes, rpc_url)| {
            get_vote_account(rpc_url).map(|(votes, rpc_url)| (stakes, votes, rpc_url))
        })
        .and_then(|(stakes, votes, rpc_url)| {
            get_stakehistory_account(rpc_url).map(|history| (stakes, votes, history))
        })
}

fn get_stake_account(rpc_url: String) -> Result<(Vec<(Pubkey, Account)>, String), ClientError> {
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url.clone(),
        Duration::from_secs(600),
        CommitmentConfig::finalized(),
    );
    rpc_client
        .get_program_accounts(&solana_sdk::stake::program::id())
        .map(|stake| (stake, rpc_url))
}

fn get_vote_account(rpc_url: String) -> Result<(Vec<(Pubkey, Account)>, String), ClientError> {
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url.clone(),
        Duration::from_secs(600),
        CommitmentConfig::finalized(),
    );
    rpc_client
        .get_program_accounts(&solana_sdk::vote::program::id())
        .map(|votes| (votes, rpc_url))
}

pub fn get_stakehistory_account(rpc_url: String) -> Result<Account, ClientError> {
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url,
        Duration::from_secs(600),
        CommitmentConfig::finalized(),
    );
    rpc_client.get_account(&solana_sdk::sysvar::stake_history::id())
}

fn get_rpc_epoch_info(rpc_url: String) -> Result<EpochInfo, ClientError> {
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url.clone(),
        Duration::from_secs(600),
        CommitmentConfig::finalized(),
    );
    rpc_client.get_epoch_info()
}

fn get_rpc_leader_schedule(
    rpc_url: String,
    slot: Option<Slot>,
) -> Result<Option<HashMap<String, Vec<usize>>>, ClientError> {
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url.clone(),
        Duration::from_secs(600),
        CommitmentConfig::finalized(),
    );
    rpc_client.get_leader_schedule(slot)
}

// pub struct BootstrapScheduleResult {
//     schedule: CalculatedSchedule,
//     vote_stakes: Vec<EpochVoteStakes>,
// }

pub fn bootstrap_current_leader_schedule(
    current_epoch_of_loading: u64,
    slots_in_epoch: u64,
) -> anyhow::Result<(CalculatedSchedule, EpochVoteStakes, EpochVoteStakes)> {
    let (current_epoch, current_epoch_stakes) =
        crate::utils::read_schedule_vote_stakes(CURRENT_EPOCH_VOTE_STAKES_FILE)?;
    let (next_epoch, next_epoch_stakes) =
        crate::utils::read_schedule_vote_stakes(NEXT_EPOCH_VOTE_STAKES_FILE)?;

    //verify that the current loaded epoch correspond to the current epoch slot
    if current_epoch_of_loading != current_epoch {
        return Err(ClientError {
            request: None,
            kind: ClientErrorKind::Custom(
                "Current epoch bootstrap file doesn't correspond to the validator current epoch."
                    .to_string(),
            ),
        }
        .into());
    }

    //calcualte leader schedule for all vote stakes.
    let current_schedule = crate::leader_schedule::calculate_leader_schedule(
        &current_epoch_stakes,
        current_epoch,
        slots_in_epoch,
    );

    let next_schedule = crate::leader_schedule::calculate_leader_schedule(
        &next_epoch_stakes,
        next_epoch,
        slots_in_epoch,
    );

    Ok((
        CalculatedSchedule {
            current: Some(LeaderScheduleData {
                schedule_by_node: LeaderScheduleGeneratedData::get_schedule_by_nodes(
                    &current_schedule,
                ),
                schedule_by_slot: current_schedule.get_slot_leaders().to_vec(),
                epoch: current_epoch,
            }),
            next: Some(LeaderScheduleData {
                schedule_by_node: LeaderScheduleGeneratedData::get_schedule_by_nodes(
                    &next_schedule,
                ),
                schedule_by_slot: next_schedule.get_slot_leaders().to_vec(),
                epoch: next_epoch,
            }),
        },
        EpochVoteStakes {
            epoch: current_epoch,
            vote_stakes: current_epoch_stakes,
        },
        EpochVoteStakes {
            epoch: next_epoch,
            vote_stakes: next_epoch_stakes,
        },
    ))
}
