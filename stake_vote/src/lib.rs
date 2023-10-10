use crate::account::AccountPretty;
use futures::Stream;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_lite_rpc_core::types::SlotStream;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::collections::HashMap;
use std::sync::Arc;
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
    rpc_client: Arc<RpcClient>,
    grpc_url: String,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let mut account_gyzer_stream = subscribe_geyzer(grpc_url).await?;
    let jh = tokio::spawn(async move {
        //Stake account management struct
        let mut stakestore = stake::StakeStore::new(STAKESTORE_INITIAL_CAPACITY);

        //Vote account management struct
        let mut votestore = vote::VoteStore::new(VOTESTORE_INITIAL_CAPACITY);

        //Init bootstrap process
        let (mut current_schedule_epoch, bootstrap_data) =
            crate::bootstrap::bootstrap_process_data(&data_cache, rpc_client.clone()).await;

        //future execution collection.
        let mut spawned_leader_schedule_task = FuturesUnordered::new();

        loop {
            tokio::select! {
                //manage confirm new slot notification to detect epoch change.
                Ok(_) = slot_notification.recv() => {
                    let new_slot = crate::utils::get_current_confirmed_slot(&data_cache).await;
                    let schedule_event = current_schedule_epoch.process_new_confirmed_slot(new_slot, &data_cache).await;
                    if bootstrap_data.done {
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
                                            //store new account stake.
                                            let current_slot = crate::utils::get_current_confirmed_slot(&data_cache).await;

                                            if let Some(account) = AccountPretty::new_from_geyzer(account, current_slot) {
                                                //log::trace!("Geyser receive new account");
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
                                                        // Generatea lot of logs. log::info!("Geyser notif VOTE account:{}", account);
                                                        let account_pubkey = account.pubkey;
                                                        //process vote accout notification
                                                        if let Err(err) = votestore.add_vote(account, current_schedule_epoch.last_slot_in_epoch) {
                                                            log::warn!("Can't add new stake from account data err:{} account:{}", err, account_pubkey);
                                                            continue;
                                                        }
                                                    }
                                                    _ => log::warn!("receive an account notification from a unknown owner:{account:?}"),
                                                }
                                            }
                                        }
                                        Some(UpdateOneof::Ping(_)) => log::trace!("UpdateOneof::Ping"),
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
                    data_schedule.next = new_leader_schedule;
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
            slots.clone(),
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
