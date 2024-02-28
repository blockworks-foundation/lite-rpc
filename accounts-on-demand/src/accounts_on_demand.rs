use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashSet;
use itertools::Itertools;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::RpcFilterType,
};
use solana_lite_rpc_accounts::account_store_interface::AccountStorageInterface;
use solana_lite_rpc_cluster_endpoints::geyser_grpc_connector::GrpcSourceConfig;
use solana_lite_rpc_core::{
    commitment_utils::Commitment,
    structures::{
        account_data::{AccountData, AccountNotificationMessage},
        account_filter::{AccountFilter, AccountFilterType, AccountFilters},
    },
};
use solana_sdk::{clock::Slot, pubkey::Pubkey};
use tokio::sync::{broadcast::Sender, RwLock};

use crate::subscription_manager::SubscriptionManger;

pub struct AccountsOnDemand {
    rpc_client: Arc<RpcClient>,
    accounts_storage: Arc<dyn AccountStorageInterface>,
    accounts_subscribed: Arc<DashSet<Pubkey>>,
    program_filters: Arc<RwLock<AccountFilters>>,
    subscription_manager: SubscriptionManger,
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
        }
    }

    pub async fn reset_subscription(&self) {
        let mut filters = self.get_filters().await;

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

    pub async fn get_filters(&self) -> AccountFilters {
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

    async fn get_account(&self, account_pk: Pubkey, commitment: Commitment) -> Option<AccountData> {
        match self
            .accounts_storage
            .get_account(account_pk, commitment)
            .await
        {
            Some(account_data) => Some(account_data),
            None => {
                // account does not exist in account store
                // first check if we have already subscribed to the required account
                if !self.accounts_subscribed.contains(&account_pk) {
                    // get account from rpc and create its subscription
                    self.accounts_subscribed.insert(account_pk);
                    self.reset_subscription().await;
                    let account_response = self
                        .rpc_client
                        .get_account_with_commitment(
                            &account_pk,
                            commitment.into_commiment_config(),
                        )
                        .await;
                    if let Ok(response) = account_response {
                        match response.value {
                            Some(account) => {
                                // update account in storage and return the account data
                                let account_data = AccountData {
                                    pubkey: account_pk,
                                    account: Arc::new(account),
                                    updated_slot: response.context.slot,
                                };
                                self.accounts_storage
                                    .update_account(account_data.clone(), commitment)
                                    .await;
                                Some(account_data)
                            }
                            // account does not exist
                            None => None,
                        }
                    } else {
                        // issue getting account, will then be updated by geyser
                        None
                    }
                } else {
                    // we have already subscribed to the account and it does not exist
                    None
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
                self.reset_subscription().await;

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
