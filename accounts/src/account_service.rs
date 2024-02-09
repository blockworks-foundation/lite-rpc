use std::{str::FromStr, sync::Arc};

use anyhow::bail;
use itertools::Itertools;
use solana_account_decoder::UiDataSliceConfig;
use solana_lite_rpc_core::{
    commitment_utils::Commitment,
    structures::account_data::{AccountData, AccountStream},
    types::BlockStream,
    AnyhowJoinHandle,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_sdk::{commitment_config::CommitmentConfig, hash::Hash};

use crate::{account_filter::AccountFilters, account_store_interface::AccountStorageInterface};

#[derive(Clone)]
pub struct AccountService {
    account_store: Arc<dyn AccountStorageInterface>,
}

impl AccountService {
    pub fn new(account_store: Arc<dyn AccountStorageInterface>) -> Self {
        Self { account_store }
    }

    pub async fn populate_from_rpc(
        &self,
        rpc_client: Arc<RpcClient>,
        filters: &AccountFilters,
        max_request_in_parallel: usize,
    ) -> anyhow::Result<()> {
        const NB_ACCOUNTS_IN_GMA: usize = 100;
        const NB_RETRY: usize = 10;
        for filter in filters.iter() {
            let accounts = rpc_client
                .get_program_accounts_with_config(
                    &filter.program_id,
                    RpcProgramAccountsConfig {
                        filters: filter.filters.clone(),
                        account_config: RpcAccountInfoConfig {
                            encoding: Some(solana_account_decoder::UiAccountEncoding::Binary),
                            data_slice: Some(UiDataSliceConfig {
                                offset: 0,
                                length: 0,
                            }),
                            commitment: Some(CommitmentConfig::finalized()),
                            min_context_slot: None,
                        },
                        with_context: None,
                    },
                )
                .await?
                .iter()
                .map(|(pk, _)| *pk)
                .collect_vec();

            for accounts in accounts.chunks(max_request_in_parallel * NB_ACCOUNTS_IN_GMA) {
                for accounts in accounts.chunks(NB_ACCOUNTS_IN_GMA) {
                    let mut fetch_accounts = vec![];
                    let mut updated_slot = 0;
                    for _ in 0..NB_RETRY {
                        let accounts = rpc_client
                            .get_multiple_accounts_with_config(
                                accounts,
                                RpcAccountInfoConfig {
                                    encoding: Some(
                                        solana_account_decoder::UiAccountEncoding::Base64,
                                    ),
                                    data_slice: None,
                                    commitment: Some(CommitmentConfig::finalized()),
                                    min_context_slot: None,
                                },
                            )
                            .await;
                        match accounts {
                            Ok(response) => {
                                fetch_accounts = response.value;
                                updated_slot = response.context.slot;
                                break;
                            }
                            Err(e) => {
                                // retry
                                log::error!("Error fetching all the accounts {e:?}, retrying");
                                continue;
                            }
                        }
                    }
                    for (index, account) in fetch_accounts.iter().enumerate() {
                        if let Some(account) = account {
                            self.account_store
                                .initilize_account(
                                    accounts[index],
                                    AccountData {
                                        account: account.clone(),
                                        updated_slot,
                                    },
                                )
                                .await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn process_account_stream(
        &self,
        mut account_stream: AccountStream,
        mut block_stream: BlockStream,
    ) -> Vec<AnyhowJoinHandle> {
        let this = self.clone();
        let processed_task = tokio::spawn(async move {
            loop {
                match account_stream.recv().await {
                    Ok(account_notification) => {
                        this.account_store
                            .update_processed_account(
                                account_notification.account_pk,
                                account_notification.data,
                                account_notification.block_hash,
                            )
                            .await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(e)) => {
                        log::error!("Account Stream Lagged by {}", e);
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        log::error!("Account Stream Broken");
                        break;
                    }
                }
            }
            bail!("Account Stream Broken");
        });

        let this = self.clone();
        let block_processing_task = tokio::spawn(async move {
            loop {
                match block_stream.recv().await {
                    Ok(block_notification) => {
                        let commitment = Commitment::from(block_notification.commitment_config);
                        let hash = Hash::from_str(block_notification.blockhash.as_str()).expect("");
                        this.account_store
                            .process_slot_data(block_notification.slot, hash, commitment)
                            .await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(e)) => {
                        log::error!("Account Stream Lagged by {}", e);
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        log::error!("Account Stream Broken");
                        break;
                    }
                }
            }
            bail!("Account Block Stream Broken");
        });

        vec![processed_task, block_processing_task]
    }
}
