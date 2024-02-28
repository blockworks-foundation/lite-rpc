use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use futures::StreamExt;
use itertools::Itertools;
use prometheus::{opts, register_int_gauge, IntGauge};
use solana_lite_rpc_accounts::account_store_interface::AccountStorageInterface;
use solana_lite_rpc_cluster_endpoints::geyser_grpc_connector::GrpcSourceConfig;
use solana_lite_rpc_core::{
    commitment_utils::Commitment,
    structures::{
        account_data::{AccountData, AccountNotificationMessage, AccountStream},
        account_filter::{AccountFilterType, AccountFilters, MemcmpFilterData},
    },
    AnyhowJoinHandle,
};
use solana_sdk::{account::Account, pubkey::Pubkey};
use tokio::sync::{
    broadcast::{self, Sender},
    watch,
};
use yellowstone_grpc_proto::geyser::{
    subscribe_request_filter_accounts_filter::Filter,
    subscribe_request_filter_accounts_filter_memcmp::Data, subscribe_update::UpdateOneof,
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
    SubscribeRequestFilterAccountsFilterMemcmp,
};

lazy_static::lazy_static! {
static ref ON_DEMAND_SUBSCRIPTION_RESTARTED: IntGauge =
        register_int_gauge!(opts!("literpc_count_account_on_demand_resubscribe", "Count number of account on demand has resubscribed")).unwrap();

        static ref ON_DEMAND_UPDATES: IntGauge =
        register_int_gauge!(opts!("literpc_count_account_on_demand_updates", "Count number of updates for account on demand")).unwrap();
}

pub struct SubscriptionManger {
    account_filter_watch: watch::Sender<AccountFilters>,
}

impl SubscriptionManger {
    pub fn new(
        grpc_sources: Vec<GrpcSourceConfig>,
        accounts_storage: Arc<dyn AccountStorageInterface>,
        account_notification_sender: Sender<AccountNotificationMessage>,
    ) -> Self {
        let (account_filter_watch, reciever) = watch::channel::<AccountFilters>(vec![]);

        let (_, mut account_stream) = create_grpc_account_streaming_tasks(grpc_sources, reciever);

        tokio::spawn(async move {
            loop {
                match account_stream.recv().await {
                    Ok(message) => {
                        ON_DEMAND_UPDATES.inc();
                        let _ = account_notification_sender.send(message.clone());
                        accounts_storage
                            .update_account(message.data, message.commitment)
                            .await;
                    }
                    Err(e) => match e {
                        broadcast::error::RecvError::Closed => {
                            panic!("Account stream channel is broken");
                        }
                        broadcast::error::RecvError::Lagged(lag) => {
                            log::error!("Account on demand stream lagged by {lag:?}, missed some account updates; continue");
                            continue;
                        }
                    },
                }
            }
        });
        Self {
            account_filter_watch,
        }
    }

    pub async fn update_subscriptions(&self, filters: AccountFilters) {
        if let Err(e) = self.account_filter_watch.send(filters) {
            log::error!(
                "Error updating accounts on demand subscription with {}",
                e.to_string()
            );
        }
    }
}

pub fn start_account_streaming_task(
    grpc_config: GrpcSourceConfig,
    accounts_filters: AccountFilters,
    account_stream_sx: broadcast::Sender<AccountNotificationMessage>,
    has_started: Arc<AtomicBool>,
) -> AnyhowJoinHandle {
    tokio::spawn(async move {
        'main_loop: loop {
            let processed_commitment = yellowstone_grpc_proto::geyser::CommitmentLevel::Processed;

            let mut subscribe_accounts: HashMap<String, SubscribeRequestFilterAccounts> =
                HashMap::new();

            for (index, accounts_filter) in accounts_filters.iter().enumerate() {
                if !accounts_filter.accounts.is_empty() {
                    subscribe_accounts.insert(
                        format!("accounts_on_demand_{index:?}"),
                        SubscribeRequestFilterAccounts {
                            account: accounts_filter
                                .accounts
                                .iter()
                                .map(|x| x.to_string())
                                .collect_vec(),
                            owner: vec![],
                            filters: vec![],
                        },
                    );
                }
                if let Some(program_id) = &accounts_filter.program_id {
                    let filters = if let Some(filters) = &accounts_filter.filters {
                        filters
                            .iter()
                            .map(|filter| match filter {
                                AccountFilterType::Datasize(size) => {
                                    SubscribeRequestFilterAccountsFilter {
                                        filter: Some(Filter::Datasize(*size)),
                                    }
                                }
                                AccountFilterType::Memcmp(memcmp) => {
                                    SubscribeRequestFilterAccountsFilter {
                                        filter: Some(Filter::Memcmp(
                                            SubscribeRequestFilterAccountsFilterMemcmp {
                                                offset: memcmp.offset,
                                                data: Some(match &memcmp.data {
                                                    MemcmpFilterData::Bytes(bytes) => {
                                                        Data::Bytes(bytes.clone())
                                                    }
                                                    MemcmpFilterData::Base58(data) => {
                                                        Data::Base58(data.clone())
                                                    }
                                                    MemcmpFilterData::Base64(data) => {
                                                        Data::Base64(data.clone())
                                                    }
                                                }),
                                            },
                                        )),
                                    }
                                }
                                AccountFilterType::TokenAccountState => {
                                    SubscribeRequestFilterAccountsFilter {
                                        filter: Some(Filter::TokenAccountState(false)),
                                    }
                                }
                            })
                            .collect_vec()
                    } else {
                        vec![]
                    };
                    subscribe_accounts.insert(
                        format!("program_accounts_on_demand_{}", program_id),
                        SubscribeRequestFilterAccounts {
                            account: vec![],
                            owner: vec![program_id.clone()],
                            filters,
                        },
                    );
                }
            }

            let subscribe_request = SubscribeRequest {
                accounts: subscribe_accounts,
                slots: Default::default(),
                transactions: Default::default(),
                blocks: Default::default(),
                blocks_meta: Default::default(),
                entry: Default::default(),
                commitment: Some(processed_commitment.into()),
                accounts_data_slice: Default::default(),
                ping: None,
            };

            log::info!(
                "Accounts on demand subscribing to {}",
                grpc_config.grpc_addr
            );
            let Ok(mut client) = yellowstone_grpc_client::GeyserGrpcClient::connect(
                grpc_config.grpc_addr.clone(),
                grpc_config.grpc_x_token.clone(),
                None,
            ) else {
                // problem connecting to grpc, retry after a sec
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };

            let Ok(mut account_stream) = client.subscribe_once2(subscribe_request).await else {
                // problem subscribing to geyser stream, retry after a sec
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };

            while let Some(message) = account_stream.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(status) => {
                        log::error!("Account on demand grpc error : {}", status.message());
                        continue;
                    },
                };
                let Some(update) = message.update_oneof else {
                    continue;
                };

                has_started.store(true, std::sync::atomic::Ordering::Relaxed);

                match update {
                    UpdateOneof::Account(account) => {
                        if let Some(account_data) = account.account {
                            let account_pk_bytes: [u8; 32] = account_data
                                .pubkey
                                .try_into()
                                .expect("Pubkey should be 32 byte long");
                            let owner: [u8; 32] = account_data
                                .owner
                                .try_into()
                                .expect("owner pubkey should be deserializable");
                            let notification = AccountNotificationMessage {
                                data: AccountData {
                                    pubkey: Pubkey::new_from_array(account_pk_bytes),
                                    account: Arc::new(Account {
                                        lamports: account_data.lamports,
                                        data: account_data.data,
                                        owner: Pubkey::new_from_array(owner),
                                        executable: account_data.executable,
                                        rent_epoch: account_data.rent_epoch,
                                    }),
                                    updated_slot: account.slot,
                                },
                                // TODO update with processed commitment / check above
                                commitment: Commitment::Processed,
                            };
                            if account_stream_sx.send(notification).is_err() {
                                // non recoverable, i.e the whole stream is being restarted
                                log::error!("Account stream broken, breaking from main loop");
                                break 'main_loop;
                            }
                        }
                    }
                    UpdateOneof::Ping(_) => {
                        log::trace!("GRPC Ping accounts stream");
                    }
                    _ => {
                        log::error!("GRPC accounts steam misconfigured");
                    }
                };
            }
        }
        Ok(())
    })
}

pub fn create_grpc_account_streaming_tasks(
    grpc_sources: Vec<GrpcSourceConfig>,
    mut account_filter_watch: watch::Receiver<AccountFilters>,
) -> (AnyhowJoinHandle, AccountStream) {
    let (account_sender, accounts_stream) = broadcast::channel::<AccountNotificationMessage>(128);

    let jh: AnyhowJoinHandle = tokio::spawn(async move {
        match account_filter_watch.changed().await {
            Ok(_) => {
                // do nothing
            }
            Err(e) => {
                log::error!("account filter watch failed with error {}", e);
                anyhow::bail!("Accounts on demand task failed");
            }
        }
        let accounts_filters = account_filter_watch.borrow_and_update().clone();

        let has_started = Arc::new(AtomicBool::new(false));

        let mut current_tasks = grpc_sources
            .iter()
            .map(|grpc_config| {
                start_account_streaming_task(
                    grpc_config.clone(),
                    accounts_filters.clone(),
                    account_sender.clone(),
                    has_started.clone(),
                )
            })
            .collect_vec();

        'check_watch: while account_filter_watch.changed().await.is_ok() {
            ON_DEMAND_SUBSCRIPTION_RESTARTED.inc();
            // wait for a second to get all the accounts to update
            tokio::time::sleep(Duration::from_secs(1)).await;
            let accounts_filters = account_filter_watch.borrow_and_update().clone();

            let has_started_new = Arc::new(AtomicBool::new(false));
            let elapsed_restart = tokio::time::Instant::now();

            let new_tasks = grpc_sources
                .iter()
                .map(|grpc_config| {
                    start_account_streaming_task(
                        grpc_config.clone(),
                        accounts_filters.clone(),
                        account_sender.clone(),
                        has_started_new.clone(),
                    )
                })
                .collect_vec();

            while !has_started_new.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(10)).await;
                if elapsed_restart.elapsed() > Duration::from_secs(60) {
                    // check if time elapsed during restart is greater than 60ms
                    log::error!("Tried to restart the accounts on demand task but failed");
                    new_tasks.iter().for_each(|x| x.abort());
                    continue 'check_watch;
                }
            }

            // abort previous tasks
            current_tasks.iter().for_each(|x| x.abort());

            current_tasks = new_tasks;
        }
        log::error!("Accounts on demand task stopped");
        anyhow::bail!("Accounts on demand task stopped");
    });

    (jh, accounts_stream)
}
