use futures::StreamExt;
use std::{collections::HashMap, time::Duration};

use geyser_grpc_connector::GrpcSourceConfig;
use itertools::Itertools;
use solana_lite_rpc_core::{
    commitment_utils::Commitment,
    structures::{
        account_data::{AccountData, AccountNotificationMessage, AccountStream},
        account_filter::{AccountFilterType, AccountFilters, MemcmpFilterData},
    },
    AnyhowJoinHandle,
};
use solana_sdk::{account::Account, pubkey::Pubkey};
use tokio::sync::broadcast;
use yellowstone_grpc_proto::geyser::{
    subscribe_request_filter_accounts_filter::Filter,
    subscribe_request_filter_accounts_filter_memcmp::Data, subscribe_update::UpdateOneof,
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
    SubscribeRequestFilterAccountsFilterMemcmp,
};

pub fn start_account_streaming_tasks(
    grpc_config: GrpcSourceConfig,
    accounts_filters: AccountFilters,
    account_stream_sx: tokio::sync::mpsc::UnboundedSender<AccountNotificationMessage>,
) -> AnyhowJoinHandle {
    tokio::spawn(async move {
        'main_loop: loop {
            // for now we can only be sure that there is one confirmed block per slot, for processed there can be multiple confirmed blocks
            // So setting commitment to confirmed
            // To do somehow make it processed, we we could get blockhash with slot it should be ideal
            let confirmed_commitment = yellowstone_grpc_proto::geyser::CommitmentLevel::Processed;

            let mut subscribe_accounts: HashMap<String, SubscribeRequestFilterAccounts> =
                HashMap::new();

            for (index, accounts_filter) in accounts_filters.iter().enumerate() {
                if !accounts_filter.accounts.is_empty() {
                    subscribe_accounts.insert(
                        format!("accounts_{index:?}"),
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
                        format!("accounts_{}", program_id),
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
                commitment: Some(confirmed_commitment.into()),
                accounts_data_slice: Default::default(),
                ping: None,
            };

            let mut client = yellowstone_grpc_client::GeyserGrpcClient::connect(
                grpc_config.grpc_addr.clone(),
                grpc_config.grpc_x_token.clone(),
                None,
            )
            .unwrap();
            let mut account_stream = client.subscribe_once2(subscribe_request).await.unwrap();

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
                                    account: Account {
                                        lamports: account_data.lamports,
                                        data: account_data.data,
                                        owner: Pubkey::new_from_array(owner),
                                        executable: account_data.executable,
                                        rent_epoch: account_data.rent_epoch,
                                    },
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
            log::error!("Grpc account subscription broken (resubscribing)");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        Ok(())
    })
}

pub fn create_grpc_account_streaming(
    grpc_sources: Vec<GrpcSourceConfig>,
    accounts_filters: AccountFilters,
) -> (AnyhowJoinHandle, AccountStream) {
    let (account_sender, accounts_stream) = broadcast::channel::<AccountNotificationMessage>(128);

    let jh: AnyhowJoinHandle = tokio::spawn(async move {
        loop {
            let (accounts_sx, mut accounts_rx) = tokio::sync::mpsc::unbounded_channel();
            grpc_sources
                .iter()
                .map(|grpc_config| {
                    start_account_streaming_tasks(
                        grpc_config.clone(),
                        accounts_filters.clone(),
                        accounts_sx.clone(),
                    )
                })
                .collect_vec();
            drop(accounts_sx);

            loop {
                match tokio::time::timeout(Duration::from_secs(60), accounts_rx.recv()).await {
                    Ok(Some(data)) => {
                        let _ = account_sender.send(data);
                    }
                    Ok(None) => {
                        log::error!("All grpc accounts channels close; restarting subscription");
                        break;
                    }
                    Err(_elapsed) => {
                        log::error!("No accounts data for a minute; restarting subscription");
                        break;
                    }
                }
            }
        }
    });

    (jh, accounts_stream)
}
