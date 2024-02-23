use std::{str::FromStr, sync::Arc};

use anyhow::bail;
use itertools::Itertools;
use solana_account_decoder::{UiAccount, UiDataSliceConfig};
use solana_lite_rpc_core::{
    commitment_utils::Commitment,
    structures::{
        account_data::{AccountData, AccountNotificationMessage, AccountStream},
        account_filter::AccountFilters,
    },
    types::BlockStream,
    AnyhowJoinHandle,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::{
    config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    response::RpcKeyedAccount,
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, slot_history::Slot};
use tokio::sync::broadcast::Sender;

use crate::account_store_interface::AccountStorageInterface;

#[derive(Clone)]
pub struct AccountService {
    account_store: Arc<dyn AccountStorageInterface>,
    pub account_notification_sender: Sender<AccountNotificationMessage>,
}

impl AccountService {
    pub fn new(
        account_store: Arc<dyn AccountStorageInterface>,
        account_notification_sender: Sender<AccountNotificationMessage>,
    ) -> Self {
        Self {
            account_store,
            account_notification_sender,
        }
    }

    pub async fn populate_from_rpc(
        &self,
        rpc_client: Arc<RpcClient>,
        filters: &AccountFilters,
        max_request_in_parallel: usize,
    ) -> anyhow::Result<()> {
        const NB_ACCOUNTS_IN_GMA: usize = 100;
        const NB_RETRY: usize = 10;
        let mut accounts = vec![];
        for filter in filters.iter() {
            if !filter.accounts.is_empty() {
                let mut f_accounts = filter
                    .accounts
                    .iter()
                    .map(|x| Pubkey::from_str(x).expect("Accounts in filters should be valid"))
                    .collect();
                accounts.append(&mut f_accounts);
            }

            if let Some(program_id) = &filter.program_id {
                let program_id =
                    Pubkey::from_str(program_id).expect("Program id in filters should be valid");
                let mut rpc_acc = rpc_client
                    .get_program_accounts_with_config(
                        &program_id,
                        RpcProgramAccountsConfig {
                            filters: filter.get_rpc_filter(),
                            account_config: RpcAccountInfoConfig {
                                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                                data_slice: Some(UiDataSliceConfig {
                                    offset: 0,
                                    length: 0,
                                }),
                                commitment: None,
                                min_context_slot: None,
                            },
                            with_context: None,
                        },
                    )
                    .await?
                    .iter()
                    .map(|(pk, _)| *pk)
                    .collect_vec();
                accounts.append(&mut rpc_acc);
            }
        }
        log::info!("Fetching {} accounts", accounts.len());
        for accounts in accounts.chunks(max_request_in_parallel * NB_ACCOUNTS_IN_GMA) {
            for accounts in accounts.chunks(NB_ACCOUNTS_IN_GMA) {
                let mut fetch_accounts = vec![];
                let mut updated_slot = 0;
                for _ in 0..NB_RETRY {
                    let accounts = rpc_client
                        .get_multiple_accounts_with_config(
                            accounts,
                            RpcAccountInfoConfig {
                                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
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
                            .initilize_account(AccountData {
                                pubkey: accounts[index],
                                account: account.clone(),
                                updated_slot,
                            })
                            .await;
                    }
                }
            }
        }
        log::info!("{} accounts successfully fetched", accounts.len());
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
                        if this
                            .account_store
                            .update_account(
                                account_notification.data.clone(),
                                account_notification.commitment,
                            )
                            .await
                        {
                            let _ = this.account_notification_sender.send(account_notification);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(e)) => {
                        log::error!(
                            "Account Stream Lagged by {}, we may have missed some account updates",
                            e
                        );
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        bail!("Account Stream Broken");
                    }
                }
            }
        });

        let this = self.clone();
        let block_processing_task = tokio::spawn(async move {
            loop {
                match block_stream.recv().await {
                    Ok(block_notification) => {
                        if block_notification.commitment_config.is_processed() {
                            // processed commitment is not processed in this loop
                            continue;
                        }
                        let commitment = Commitment::from(block_notification.commitment_config);
                        let updated_accounts = this
                            .account_store
                            .process_slot_data(block_notification.slot, commitment)
                            .await;
                        for data in updated_accounts {
                            let _ = this
                                .account_notification_sender
                                .send(AccountNotificationMessage { data, commitment });
                        }
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

    pub fn convert_account_data_to_ui_account(
        account_data: &AccountData,
        config: Option<RpcAccountInfoConfig>,
    ) -> UiAccount {
        let encoding = config
            .as_ref()
            .map(|c| c.encoding)
            .unwrap_or_default()
            .unwrap_or(solana_account_decoder::UiAccountEncoding::Base64);
        let data_slice = config.as_ref().map(|c| c.data_slice).unwrap_or_default();
        UiAccount::encode(
            &account_data.pubkey,
            &account_data.account,
            encoding,
            None,
            data_slice,
        )
    }

    pub async fn get_account(
        &self,
        account: Pubkey,
        config: Option<RpcAccountInfoConfig>,
    ) -> anyhow::Result<(Slot, Option<UiAccount>)> {
        let commitment = config
            .as_ref()
            .map(|config| config.commitment.unwrap_or_default())
            .unwrap_or_default();

        let commitment = Commitment::from(commitment);

        if let Some(account_data) = self.account_store.get_account(account, commitment).await {
            let ui_account =
                Self::convert_account_data_to_ui_account(&account_data, config.clone());

            // if minimum context slot is not satisfied return Null
            let minimum_context_slot = config
                .as_ref()
                .map(|c| c.min_context_slot.unwrap_or_default())
                .unwrap_or_default();
            if minimum_context_slot <= account_data.updated_slot {
                Ok((account_data.updated_slot, Some(ui_account)))
            } else {
                Ok((account_data.updated_slot, None))
            }
        } else {
            bail!(
                "Account {} does not satisfy any configured filters",
                account.to_string()
            )
        }
    }

    pub async fn get_program_accounts(
        &self,
        program_id: Pubkey,
        config: Option<RpcProgramAccountsConfig>,
    ) -> anyhow::Result<(Slot, Vec<RpcKeyedAccount>)> {
        let account_filter = config
            .as_ref()
            .map(|x| x.filters.clone())
            .unwrap_or_default();
        let commitment = config
            .as_ref()
            .map(|c| c.account_config.commitment)
            .unwrap_or_default()
            .unwrap_or_default();
        let commitment = Commitment::from(commitment);

        let program_accounts = self
            .account_store
            .get_program_accounts(program_id, account_filter, commitment)
            .await;
        if let Some(program_accounts) = program_accounts {
            let min_context_slot = config
                .as_ref()
                .map(|c| {
                    if c.with_context.unwrap_or_default() {
                        c.account_config.min_context_slot
                    } else {
                        None
                    }
                })
                .unwrap_or_default()
                .unwrap_or_default();
            let slot = program_accounts
                .iter()
                .map(|program_account| program_account.updated_slot)
                .max()
                .unwrap_or_default();
            let acc_config = config.map(|c| c.account_config);
            let rpc_keyed_accounts = program_accounts
                .iter()
                .filter_map(|account_data| {
                    if account_data.updated_slot >= min_context_slot {
                        Some(RpcKeyedAccount {
                            pubkey: account_data.pubkey.to_string(),
                            account: Self::convert_account_data_to_ui_account(
                                account_data,
                                acc_config.clone(),
                            ),
                        })
                    } else {
                        None
                    }
                })
                .collect_vec();
            Ok((slot, rpc_keyed_accounts))
        } else {
            bail!(
                "Program id {} does not satisfy any configured filters",
                program_id.to_string()
            )
        }
    }
}
