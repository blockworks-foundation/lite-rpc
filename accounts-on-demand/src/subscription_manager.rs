use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::StreamExt;
use itertools::Itertools;
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
    RwLock,
};
use yellowstone_grpc_proto::geyser::{
    subscribe_request_filter_accounts_filter::Filter,
    subscribe_request_filter_accounts_filter_memcmp::Data, subscribe_update::UpdateOneof,
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
    SubscribeRequestFilterAccountsFilterMemcmp,
};

pub struct SubscriptionManger {
    accounts_filters: Arc<RwLock<AccountFilters>>,
    restart_sender: broadcast::Sender<()>,
}

impl SubscriptionManger {
    pub fn new(
        grpc_sources: Vec<GrpcSourceConfig>,
        accounts_storage: Arc<dyn AccountStorageInterface>,
        account_notification_sender: Sender<AccountNotificationMessage>,
    ) -> Self {
        let (restart_sender, recver) = broadcast::channel(1);
        let accounts_filters = Arc::new(RwLock::new(vec![]));

        let (_, mut account_stream) =
            create_grpc_account_streaming_tasks(grpc_sources, accounts_filters.clone(), recver);

        let task_restart_sender = restart_sender.clone();
        tokio::spawn(async move {
            loop {
                match tokio::time::timeout(Duration::from_secs(60), account_stream.recv()).await {
                    Ok(Ok(message)) => {
                        let _ = account_notification_sender.send(message.clone());
                        accounts_storage
                            .update_account(message.data, message.commitment)
                            .await;
                    }
                    Ok(Err(e)) => match e {
                        broadcast::error::RecvError::Closed => {
                            panic!("Account stream channel is broken");
                        }
                        broadcast::error::RecvError::Lagged(lag) => {
                            log::error!("Account on demand stream lagged by {lag:?}, missed some account updates; continue");
                            continue;
                        }
                    },
                    Err(_elapsed) => {
                        let _ = task_restart_sender.send(());
                    }
                }
            }
        });
        Self {
            accounts_filters,
            restart_sender,
        }
    }

    pub async fn update_subscriptions(&self, filters: AccountFilters) {
        *self.accounts_filters.write().await = filters;
        let _ = self.restart_sender.send(());
    }
}

pub fn start_account_streaming_tasks(
    grpc_config: GrpcSourceConfig,
    accounts_filters: Arc<RwLock<AccountFilters>>,
    mut restart_channel: broadcast::Receiver<()>,
    account_stream_sx: broadcast::Sender<AccountNotificationMessage>,
) {
    tokio::spawn(async move {
        'main_loop: loop {
            let processed_commitment = yellowstone_grpc_proto::geyser::CommitmentLevel::Processed;

            let mut subscribe_accounts: HashMap<String, SubscribeRequestFilterAccounts> =
                HashMap::new();

            let accounts_filters = accounts_filters.read().await.clone();
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
                let message = message.unwrap();
                let Some(update) = message.update_oneof else {
                    continue;
                };

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

                match restart_channel.try_recv() {
                    Ok(_) => {
                        log::info!("Restarting account subscription");
                        break;
                    }
                    Err(e) => match e {
                        broadcast::error::TryRecvError::Empty => {}
                        broadcast::error::TryRecvError::Closed => {}
                        broadcast::error::TryRecvError::Lagged(_) => {
                            // remove message in the queue
                            let _ = restart_channel.try_recv();
                            break;
                        }
                    },
                }
            }
        }
    });
}

pub fn create_grpc_account_streaming_tasks(
    grpc_sources: Vec<GrpcSourceConfig>,
    accounts_filters: Arc<RwLock<AccountFilters>>,
    restart_channel: broadcast::Receiver<()>,
) -> (AnyhowJoinHandle, AccountStream) {
    let (account_sender, accounts_stream) = broadcast::channel::<AccountNotificationMessage>(128);

    let jh: AnyhowJoinHandle = tokio::spawn(async move {
        // wait for atleast one filter
        while accounts_filters.read().await.is_empty() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        grpc_sources
            .iter()
            .map(|grpc_config| {
                start_account_streaming_tasks(
                    grpc_config.clone(),
                    accounts_filters.clone(),
                    restart_channel.resubscribe(),
                    account_sender.clone(),
                )
            })
            .collect_vec();
        Ok(())
    });

    (jh, accounts_stream)
}
