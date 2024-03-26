use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use dashmap::DashSet;
use futures::lock::Mutex;
use itertools::Itertools;
use prometheus::{opts, register_int_gauge, IntGauge};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::RpcFilterType,
};
use solana_lite_rpc_accounts::account_store_interface::{
    AccountLoadingError, AccountStorageInterface,
};
use solana_lite_rpc_cluster_endpoints::geyser_grpc_connector::GrpcSourceConfig;
use solana_lite_rpc_core::{
    commitment_utils::Commitment,
    structures::{
        account_data::{AccountData, AccountNotificationMessage},
        account_filter::{AccountFilter, AccountFilterType, AccountFilters},
    },
};
use solana_sdk::{clock::Slot, pubkey::Pubkey};
use tokio::sync::{broadcast::Sender, Notify, RwLock};

use crate::subscription_manager::SubscriptionManger;

lazy_static::lazy_static! {
    static ref NUMBER_OF_ACCOUNTS_ON_DEMAND: IntGauge =
       register_int_gauge!(opts!("literpc_number_of_accounts_on_demand", "Number of accounts on demand")).unwrap();

    static ref NUMBER_OF_PROGRAM_FILTERS_ON_DEMAND: IntGauge =
        register_int_gauge!(opts!("literpc_number_of_program_filters_on_demand", "Number of program filters on demand")).unwrap();
}

const RETRY_FETCHING_ACCOUNT: usize = 10;

pub struct AccountsOnDemand {
    rpc_client: Arc<RpcClient>,
    accounts_storage: Arc<dyn AccountStorageInterface>,
    accounts_subscribed: Arc<DashSet<Pubkey>>,
    program_filters: Arc<RwLock<AccountFilters>>,
    subscription_manager: SubscriptionManger,
    accounts_is_loading: Arc<Mutex<HashMap<Pubkey, Arc<Notify>>>>,
}

impl AccountsOnDemand {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        grpc_sources: Vec<GrpcSourceConfig>,
        accounts_storage: Arc<dyn AccountStorageInterface>,
        account_notification_sender: Sender<AccountNotificationMessage>,
    ) -> Self {
        Self {
            rpc_client,
            accounts_storage: accounts_storage.clone(),
            accounts_subscribed: Arc::new(DashSet::new()),
            program_filters: Arc::new(RwLock::new(vec![])),
            subscription_manager: SubscriptionManger::new(
                grpc_sources,
                accounts_storage,
                account_notification_sender,
            ),
            accounts_is_loading: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn refresh_subscription(&self) {
        let mut filters = self.get_filters().await;
        NUMBER_OF_PROGRAM_FILTERS_ON_DEMAND.set(filters.len() as i64);
        NUMBER_OF_ACCOUNTS_ON_DEMAND.set(self.accounts_subscribed.len() as i64);
        // add additional filters related to accounts
        for accounts in &self
            .accounts_subscribed
            .iter()
            .map(|x| x.to_string())
            .chunks(100)
        {
            let account_filter = AccountFilter {
                accounts: accounts.collect(),
                program_id: None,
                filters: None,
            };
            filters.push(account_filter);
        }

        self.subscription_manager
            .update_subscriptions(filters)
            .await;
    }

    async fn get_filters(&self) -> AccountFilters {
        self.program_filters.read().await.clone()
    }
}

#[async_trait]
impl AccountStorageInterface for AccountsOnDemand {
    async fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool {
        self.accounts_storage
            .update_account(account_data, commitment)
            .await
    }

    async fn initilize_or_update_account(&self, account_data: AccountData) {
        self.accounts_storage
            .initilize_or_update_account(account_data)
            .await
    }

    async fn get_account(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<AccountData>, AccountLoadingError> {
        match self
            .accounts_storage
            .get_account(account_pk, commitment)
            .await?
        {
            Some(account_data) => Ok(Some(account_data)),
            None => {
                // account does not exist in account store
                // first check if we have already subscribed to the required account
                // This is to avoid resetting geyser subscription because of accounts that do not exists.
                let mut lk = self.accounts_is_loading.lock().await;
                match lk.get(&account_pk).cloned() {
                    Some(loading_account) => {
                        drop(lk);
                        match tokio::time::timeout(
                            Duration::from_secs(10),
                            loading_account.notified(),
                        )
                        .await
                        {
                            Ok(_) => {
                                self.accounts_storage
                                    .get_account(account_pk, commitment)
                                    .await
                            }
                            Err(_timeout) => Err(AccountLoadingError::OperationTimeOut),
                        }
                    }
                    None => {
                        // account is not loading
                        if self.accounts_subscribed.contains(&account_pk) {
                            // account was already tried to be loaded but does not exists
                            Ok(None)
                        } else {
                            // update account loading map
                            // create a notify for accounts under loading
                            lk.insert(account_pk, Arc::new(Notify::new()));
                            self.accounts_subscribed.insert(account_pk);
                            drop(lk);
                            self.refresh_subscription().await;
                            let mut return_value = None;
                            for _ in 0..RETRY_FETCHING_ACCOUNT {
                                let account_response = self
                                    .rpc_client
                                    .get_account_with_commitment(
                                        &account_pk,
                                        commitment.into_commiment_config(),
                                    )
                                    .await;
                                match account_response {
                                    Ok(response) => {
                                        if let Some(account) = response.value {
                                            // update account in storage and return the account data
                                            let account_data = AccountData {
                                                pubkey: account_pk,
                                                account: Arc::new(account),
                                                updated_slot: response.context.slot,
                                            };
                                            self.accounts_storage
                                                .update_account(account_data.clone(), commitment)
                                                .await;
                                            return_value = Some(account_data);
                                            break;
                                        } else {
                                            // account does not exist
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "Error fetching account {} {e:?}",
                                            account_pk.to_string()
                                        );
                                    }
                                }
                            }
                            // update loading lock
                            {
                                let mut write_lock = self.accounts_is_loading.lock().await;
                                let notify = write_lock.remove(&account_pk);
                                drop(write_lock);
                                if let Some(notify) = notify {
                                    notify.notify_waiters();
                                }
                            }
                            Ok(return_value)
                        }
                    }
                }
            }
        }
    }

    async fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        filters: Option<Vec<RpcFilterType>>,
        commitment: Commitment,
    ) -> Option<Vec<AccountData>> {
        match self
            .accounts_storage
            .get_program_accounts(program_pubkey, filters.clone(), commitment)
            .await
        {
            Some(accounts) => {
                // filter is already present
                Some(accounts)
            }
            None => {
                // check if filter is already present
                let current_filters = self.get_filters().await;
                let account_filter = AccountFilter {
                    accounts: vec![],
                    program_id: Some(program_pubkey.to_string()),
                    filters: filters
                        .clone()
                        .map(|v| v.iter().map(AccountFilterType::from).collect()),
                };
                if current_filters.contains(&account_filter) {
                    // filter already exisits / there is no account data
                    return None;
                }

                // add into filters
                {
                    let mut writelk = self.program_filters.write().await;
                    writelk.push(account_filter.clone());
                }
                self.refresh_subscription().await;

                let rpc_response = self
                    .rpc_client
                    .get_program_accounts_with_config(
                        &program_pubkey,
                        RpcProgramAccountsConfig {
                            filters,
                            account_config: RpcAccountInfoConfig {
                                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                                data_slice: None,
                                commitment: Some(commitment.into_commiment_config()),
                                min_context_slot: None,
                            },
                            with_context: None,
                        },
                    )
                    .await;
                match rpc_response {
                    Ok(program_accounts) => {
                        let program_accounts = program_accounts
                            .iter()
                            .map(|(pk, account)| AccountData {
                                pubkey: *pk,
                                account: Arc::new(account.clone()),
                                updated_slot: 0,
                            })
                            .collect_vec();
                        // add fetched accounts into cache
                        for account_data in &program_accounts {
                            self.accounts_storage
                                .update_account(account_data.clone(), commitment)
                                .await;
                        }
                        Some(program_accounts)
                    }
                    Err(e) => {
                        log::warn!("Got error while getting program accounts with {e:?}");
                        None
                    }
                }
            }
        }
    }

    async fn process_slot_data(&self, slot: Slot, commitment: Commitment) -> Vec<AccountData> {
        self.accounts_storage
            .process_slot_data(slot, commitment)
            .await
    }
}
