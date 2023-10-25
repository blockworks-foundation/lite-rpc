use crate::account::AccountPretty;
use crate::bootstrap::BootstrapEvent;
use crate::leader_schedule::LeaderScheduleGeneratedData;
use crate::utils::{Takable, TakeResult};
use futures::Stream;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_lite_rpc_core::structures::leaderschedule::GetVoteAccountsConfig;
use solana_lite_rpc_core::structures::leaderschedule::LeaderScheduleData;
use solana_lite_rpc_core::types::SlotStream;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::response::RpcVoteAccountStatus;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterAccounts;
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterBlocksMeta;
use yellowstone_grpc_proto::prelude::SubscribeUpdate;
use yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeRequestFilterSlots};
use yellowstone_grpc_proto::tonic::Status;

mod account;
mod bootstrap;
mod epoch;
mod leader_schedule;
mod stake;
mod utils;
mod vote;

const STAKESTORE_INITIAL_CAPACITY: usize = 600000;
const VOTESTORE_INITIAL_CAPACITY: usize = 600000;

type Slot = u64;

pub async fn start_stakes_and_votes_loop(
    mut data_cache: DataCache,
    mut slot_notification: SlotStream,
    mut vote_account_rpc_request: Receiver<(
        GetVoteAccountsConfig,
        tokio::sync::oneshot::Sender<RpcVoteAccountStatus>,
    )>,
    rpc_client: Arc<RpcClient>,
    grpc_url: String,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    log::info!("Start Stake and Vote loop.");
    let mut account_gyzer_stream = subscribe_geyzer(grpc_url).await?;
    log::info!("Stake and Vote geyzer subscription done.");
    let jh = tokio::spawn(async move {
        //Stake account management struct
        let mut stakestore = stake::StakeStore::new(STAKESTORE_INITIAL_CAPACITY);

        //Vote account management struct
        let mut votestore = vote::VoteStore::new(VOTESTORE_INITIAL_CAPACITY);

        //Init bootstrap process
        let mut current_schedule_epoch =
            crate::bootstrap::bootstrap_scheduleepoch_data(&data_cache).await;

        //future execution collection.
        let mut spawned_leader_schedule_task = FuturesUnordered::new();
        let mut spawned_bootstrap_task = FuturesUnordered::new();
        let mut rpc_notify_task = FuturesUnordered::new();
        let mut rpc_exec_task = FuturesUnordered::new();
        let jh = tokio::spawn(async move {
            BootstrapEvent::InitBootstrap {
                sleep_time: 1,
                rpc_url: rpc_client.url(),
            }
        });
        spawned_bootstrap_task.push(jh);

        let mut bootstrap_done = false;
        let mut pending_rpc_request = vec![];

        loop {
            tokio::select! {
                //manage confirm new slot notification to detect epoch change.
                Ok(_) = slot_notification.recv() => {
                    //log::info!("Stake and Vote receive a slot.");
                    let new_slot = crate::utils::get_current_confirmed_slot(&data_cache).await;
                    let schedule_event = current_schedule_epoch.process_new_confirmed_slot(new_slot, &data_cache).await;
                    if bootstrap_done {
                        if let Some(init_event) = schedule_event {
                            crate::leader_schedule::run_leader_schedule_events(
                                init_event,
                                &mut spawned_leader_schedule_task,
                                &mut stakestore,
                                &mut votestore,
                            );
                        }
                    }
                }
                Some((config, return_channel)) = vote_account_rpc_request.recv() => {
                    pending_rpc_request.push(return_channel);
                    let current_slot = crate::utils::get_current_confirmed_slot(&data_cache).await;
                    let vote_accounts = votestore.vote_stakes_for_epoch(0); //TODO define epoch storage.
                    match votestore.votes.take() {
                        TakeResult::Map(votes) => {
                            let jh = tokio::task::spawn_blocking({
                                move || {
                                    let rpc_vote_accounts = crate::vote::get_rpc_vote_accounts_info(
                                        current_slot,
                                        &votes,
                                        &vote_accounts.as_ref().unwrap().vote_stakes, //TODO put in take.
                                        config,
                                    );
                                    (votes, vote_accounts, rpc_vote_accounts)
                                }
                            });
                            rpc_exec_task.push(jh);
                        }
                        TakeResult::Taken(mut stake_notify) => {
                            let notif_jh = tokio::spawn({
                                async move {
                                    stake_notify.pop().unwrap().notified().await;
                                    (current_slot, vote_accounts, config)
                                }
                            });
                            rpc_notify_task.push(notif_jh);
                        }
                    }
                }
                //manage rpc waiting request notification.
                Some(Ok((votes, vote_accounts, rpc_vote_accounts))) = rpc_exec_task.next() =>  {
                    if let Err(err) = votestore.votes.merge(votes) {
                        log::info!("Error during  RPC get vote account merge:{err}");
                    }

                    //avoid clone on the first request
                    //TODO change the logic use take less one.
                    if pending_rpc_request.len() == 1 {
                        if let Err(_) = pending_rpc_request.pop().unwrap().send(rpc_vote_accounts.clone()) {
                            log::error!("Vote accounts RPC channel send closed.");
                        }
                    } else {
                        for return_channel in pending_rpc_request.drain(..) {
                            if let Err(_) = return_channel.send(rpc_vote_accounts.clone()) {
                                log::error!("Vote accounts RPC channel send closed.");
                            }
                        }
                    }
                }
                //manage rpc waiting request notification.
                Some(Ok((current_slot, vote_accounts, config))) = rpc_notify_task.next() =>  {
                    match votestore.votes.take() {
                        TakeResult::Map(votes) => {
                            let jh = tokio::task::spawn_blocking({
                                move || {
                                    let rpc_vote_accounts = crate::vote::get_rpc_vote_accounts_info(
                                        current_slot,
                                        &votes,
                                        &vote_accounts.as_ref().unwrap().vote_stakes, //TODO put in take.
                                        config,
                                    );
                                    (votes, vote_accounts, rpc_vote_accounts)
                                }
                            });
                            rpc_exec_task.push(jh);
                        }
                        TakeResult::Taken(mut stake_notify) => {
                            let notif_jh = tokio::spawn({
                                async move {
                                    stake_notify.pop().unwrap().notified().await;
                                     (current_slot, vote_accounts, config)
                                }
                            });
                            rpc_notify_task.push(notif_jh);
                        }
                    }
                }
                //manage geyser account notification
                //Geyser delete account notification patch must be installed on the validator.
                //see https://github.com/solana-labs/solana/pull/33292
                ret = account_gyzer_stream.next() => {
                    match ret {
                         Some(message) => {
                            //process the message
                            match message {
                                Ok(msg) => {
                                    match msg.update_oneof {
                                        Some(UpdateOneof::Account(account)) => {
                                            // log::info!("Stake and Vote geyzer receive an account:{}.",
                                            //     account.account.clone().map(|a|
                                            //         solana_sdk::pubkey::Pubkey::try_from(a.pubkey).map(|k| k.to_string())
                                            //         .unwrap_or("bad pubkey".to_string()).to_string())
                                            //         .unwrap_or("no content".to_string())
                                            // );
                                            //store new account stake.
                                            let current_slot = crate::utils::get_current_confirmed_slot(&data_cache).await;

                                            if let Some(account) = AccountPretty::new_from_geyzer(account, current_slot) {
                                                match account.owner {
                                                    solana_sdk::stake::program::ID => {
                                                        log::info!("Geyser notif stake account:{}", account);
                                                        if let Err(err) = stakestore.notify_stake_change(
                                                            account,
                                                            current_schedule_epoch.last_slot_in_epoch,
                                                        ) {
                                                            log::warn!("Can't add new stake from account data err:{}", err);
                                                            continue;
                                                        }
                                                    }
                                                    solana_sdk::vote::program::ID => {
                                                        //log::info!("Geyser notif VOTE account:{}", account);
                                                        let account_pubkey = account.pubkey;
                                                        //process vote accout notification
                                                        if let Err(err) = votestore.notify_vote_change(account, current_schedule_epoch.last_slot_in_epoch) {
                                                            log::warn!("Can't add new stake from account data err:{} account:{}", err, account_pubkey);
                                                            continue;
                                                        }
                                                    }
                                                    _ => log::warn!("receive an account notification from a unknown owner:{account:?}"),
                                                }
                                            }
                                        }
                                        Some(UpdateOneof::Ping(_)) => log::trace!("UpdateOneof::Ping"),
                                        Some(UpdateOneof::Slot(slot)) => {
                                            log::trace!("Receive slot slot: {slot:?}");
                                        }
                                        bad_msg => {
                                            log::info!("Geyser stream unexpected message received:{:?}", bad_msg);
                                        }
                                    }
                                }
                                Err(error) => {
                                    log::error!("Geyser stream receive an error has message: {error:?}, try to reconnect and resynchronize.");
                                    //todo reconnect and resynchronize.
                                    //break;
                                }
                            }
                         }
                         None => {
                            //TODO Restart geyser connection and the bootstrap.
                            log::error!("The geyser stream close try to reconnect and resynchronize.");
                            break;
                         }
                    }
                }
                //manage bootstrap event
                Some(Ok(event)) = spawned_bootstrap_task.next() =>  {
                    match crate::bootstrap::run_bootstrap_events(event, &mut spawned_bootstrap_task, &mut stakestore, &mut votestore, current_schedule_epoch.slots_in_epoch) {
                        Ok(Some(boot_res))=> {

                            match boot_res {
                                Ok(current_schedule_data) => {
                                    //let data_schedule = Arc::make_mut(&mut data_cache.leader_schedule);

                                    data_cache.leader_schedule = Arc::new(current_schedule_data);
                                     bootstrap_done = true;
                                }
                                Err(err) => {
                                    log::warn!("Error during current leader schedule bootstrap from files:{err}")
                                }
                            }

                        },
                        Ok(None) => (),
                        Err(err) => log::error!("Stake / Vote Account bootstrap fail because '{err}'"),
                    }
                }
                //Manage leader schedule generation process
                Some(Ok(event)) = spawned_leader_schedule_task.next() =>  {
                    let new_leader_schedule = crate::leader_schedule::run_leader_schedule_events(
                        event,
                        &mut spawned_leader_schedule_task,
                        &mut stakestore,
                        &mut votestore,
                    );
                    //clone old schedule values is there's other use.
                    //only done once epoch. Avoid to use a Mutex.
                    let data_schedule = Arc::make_mut(&mut data_cache.leader_schedule);
                    data_schedule.current = data_schedule.next.take();
                    match new_leader_schedule {
                        //TODO use vote_stakes for vote accounts RPC call.
                        Some(schedule_data) => {
                            let new_schedule_data = LeaderScheduleData{
                                    schedule_by_node: LeaderScheduleGeneratedData::get_schedule_by_nodes(&schedule_data.schedule),
                                    schedule_by_slot: schedule_data.schedule.get_slot_leaders().to_vec(),
                                    epoch: schedule_data.epoch
                            };
                            data_schedule.next = Some(new_schedule_data);
                        }
                        None => {
                            log::warn!("Error during schedule calculus. No schedule for this epoch.");
                            data_schedule.next = None;
                        }
                    };
                }
            }
        }
    });
    Ok(jh)
}

//subscribe Geyser grpc
async fn subscribe_geyzer(
    grpc_url: String,
) -> anyhow::Result<impl Stream<Item = Result<SubscribeUpdate, Status>>> {
    let mut client = GeyserGrpcClient::connect(grpc_url, None::<&'static str>, None)?;
    //slot subscription
    let mut slots = HashMap::new();
    slots.insert("client".to_string(), SubscribeRequestFilterSlots {});

    //account subscription
    let mut accounts: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();
    accounts.insert(
        "client".to_owned(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![
                solana_sdk::stake::program::ID.to_string(),
                solana_sdk::vote::program::ID.to_string(),
            ],
            filters: vec![],
        },
    );

    //block Meta subscription filter
    let mut blocks_meta = HashMap::new();
    blocks_meta.insert("client".to_string(), SubscribeRequestFilterBlocksMeta {});

    let confirmed_stream = client
        .subscribe_once(
            Default::default(), //slots
            //slots.clone(),
            accounts.clone(),   //accounts
            Default::default(), //tx
            Default::default(), //entry
            Default::default(), //full block
            Default::default(), //block meta
            Some(CommitmentLevel::Confirmed),
            vec![],
        )
        .await?;

    Ok(confirmed_stream)
}
