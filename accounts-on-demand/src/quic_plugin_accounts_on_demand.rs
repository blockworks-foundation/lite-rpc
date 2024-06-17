use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use futures::lock::Mutex;
use prometheus::{opts, register_int_gauge, IntGauge};
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_filter::RpcFilterType};
use solana_lite_rpc_accounts::account_store_interface::{
    AccountLoadingError, AccountStorageInterface,
};
use solana_lite_rpc_core::{
    commitment_utils::Commitment,
    structures::account_data::{Account, AccountData, CompressionMethod},
};
use solana_sdk::{clock::Slot, pubkey::Pubkey};
use tokio::sync::Notify;

use crate::mutable_filter_store::MutableFilterStore;

lazy_static::lazy_static! {
    static ref NUMBER_OF_ACCOUNTS_ON_DEMAND: IntGauge =
       register_int_gauge!(opts!("literpc_number_of_accounts_on_demand", "Number of accounts on demand")).unwrap();

    static ref NUMBER_OF_PROGRAM_FILTERS_ON_DEMAND: IntGauge =
        register_int_gauge!(opts!("literpc_number_of_program_filters_on_demand", "Number of program filters on demand")).unwrap();
}

const RETRY_FETCHING_ACCOUNT: usize = 10;

pub struct QuicPluginAccountsOnDemand {
    rpc_client: Arc<RpcClient>,
    quic_plugin_client: quic_geyser_client::non_blocking::client::Client,
    mutable_filters: Arc<MutableFilterStore>,
    accounts_storage: Arc<dyn AccountStorageInterface>,
    accounts_in_loading: Arc<Mutex<HashMap<Pubkey, Arc<Notify>>>>,
}

impl QuicPluginAccountsOnDemand {
    pub fn new(
        quic_plugin_client: quic_geyser_client::non_blocking::client::Client,
        rpc_client: Arc<RpcClient>,
        mutable_filters: Arc<MutableFilterStore>,
        accounts_storage: Arc<dyn AccountStorageInterface>,
    ) -> Self {
        Self {
            quic_plugin_client,
            rpc_client,
            mutable_filters,
            accounts_storage: accounts_storage.clone(),
            accounts_in_loading: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl AccountStorageInterface for QuicPluginAccountsOnDemand {
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
                let mut lk = self.accounts_in_loading.lock().await;
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
                        if self.mutable_filters.contains_account(account_pk).await {
                            // account was already tried to be loaded but does not exists
                            Ok(None)
                        } else {
                            // update account loading map
                            // create a notify for accounts under loading
                            lk.insert(account_pk, Arc::new(Notify::new()));

                            let mut accounts_to_subscribe = HashSet::new();
                            accounts_to_subscribe.insert(account_pk);
                            if let Err(e) = self
                                .quic_plugin_client
                                .subscribe(vec![quic_geyser_common::filters::Filter::Account(
                                    quic_geyser_common::filters::AccountFilter {
                                        accounts: Some(accounts_to_subscribe),
                                        owner: None,
                                        filter: None,
                                    },
                                )])
                                .await
                            {
                                log::error!("error subscribing to account subscription : {e:?}")
                            }

                            log::info!("Accounts on demand loading: {}", account_pk.to_string());
                            drop(lk);
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
                                                account: Arc::new(Account::from_solana_account(
                                                    account,
                                                    CompressionMethod::Lz4(1),
                                                )),
                                                updated_slot: response.context.slot,
                                                write_version: 0,
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
                                let mut write_lock = self.accounts_in_loading.lock().await;
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
        _program_pubkey: Pubkey,
        _filters: Option<Vec<RpcFilterType>>,
        _commitment: Commitment,
    ) -> Option<Vec<AccountData>> {
        // accounts on demand will not fetch gPA if they do not exist
        todo!()
    }

    async fn process_slot_data(&self, slot: Slot, commitment: Commitment) -> Vec<AccountData> {
        self.accounts_storage
            .process_slot_data(slot, commitment)
            .await
    }
}
