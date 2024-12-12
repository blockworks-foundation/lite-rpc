use std::{str::FromStr, sync::Arc};

use anyhow::bail;
use itertools::Itertools;
use prometheus::{opts, register_int_gauge, IntGauge};
use solana_account_decoder::{UiAccount, UiDataSliceConfig};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::{
    config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    response::RpcKeyedAccount,
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, slot_history::Slot};
use tokio::sync::broadcast::Sender;

use solana_lite_rpc_core::types::BlockInfoStream;
use solana_lite_rpc_core::{
    commitment_utils::Commitment,
    structures::{
        account_data::{AccountData, AccountNotificationMessage, AccountStream},
        account_filter::AccountFilters,
    },
    AnyhowJoinHandle,
};

use crate::account_store_interface::{AccountLoadingError, AccountStorageInterface};

lazy_static::lazy_static! {
    static ref ACCOUNT_UPDATES: IntGauge =
       register_int_gauge!(opts!("literpc_accounts_updates", "Account Updates by lite-rpc service")).unwrap();
    static ref ACCOUNT_UPDATES_CONFIRMED: IntGauge =
       register_int_gauge!(opts!("literpc_accounts_updates_confirmed", "Account Updates by lite-rpc service")).unwrap();
    static ref ACCOUNT_UPDATES_FINALIZED: IntGauge =
       register_int_gauge!(opts!("literpc_accounts_updates_finalized", "Account Updates by lite-rpc service")).unwrap();

    static ref GET_PROGRAM_ACCOUNT_CALLED: IntGauge =
       register_int_gauge!(opts!("literpc_gpa_called", "Account Updates by lite-rpc service")).unwrap();

    static ref GET_ACCOUNT_CALLED: IntGauge =
       register_int_gauge!(opts!("literpc_get_account_called", "Account Updates by lite-rpc service")).unwrap();
}

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
                            sort_results: None,
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
                            .initilize_or_update_account(AccountData {
                                pubkey: accounts[index],
                                account: Arc::new(account.clone()),
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
        mut blockinfo_stream: BlockInfoStream,
    ) -> Vec<AnyhowJoinHandle> {
        let this = self.clone();
        let processed_task = tokio::spawn(async move {
            loop {
                match account_stream.recv().await {
                    Ok(account_notification) => {
                        ACCOUNT_UPDATES.inc();
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
                match blockinfo_stream.recv().await {
                    Ok(block_info) => {
                        let commitment = Commitment::from(block_info.commitment_config);
                        let updated_accounts = this
                            .account_store
                            .process_slot_data(block_info.slot, commitment)
                            .await;
                        match commitment {
                            Commitment::Processed => {}
                            Commitment::Confirmed => {
                                ACCOUNT_UPDATES_CONFIRMED.add(updated_accounts.len() as i64);
                            }
                            Commitment::Finalized => {
                                ACCOUNT_UPDATES_FINALIZED.add(updated_accounts.len() as i64)
                            }
                        }

                        for data in updated_accounts {
                            let _ = this
                                .account_notification_sender
                                .send(AccountNotificationMessage { data, commitment });
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(e)) => {
                        log::error!("Block Stream Lagged to update accounts by {}", e);
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        log::error!("Block Stream Broken");
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
            account_data.account.as_ref(),
            encoding,
            None,
            data_slice,
        )
    }

    pub async fn get_account(
        &self,
        account: Pubkey,
        config: Option<RpcAccountInfoConfig>,
    ) -> Result<(Slot, Option<UiAccount>), AccountLoadingError> {
        GET_ACCOUNT_CALLED.inc();
        let commitment = config
            .as_ref()
            .map(|config| config.commitment.unwrap_or_default())
            .unwrap_or_default();

        let commitment = Commitment::from(commitment);

        if let Some(account_data) = self.account_store.get_account(account, commitment).await? {
            // if minimum context slot is not satisfied return Null
            let minimum_context_slot = config
                .as_ref()
                .map(|c| c.min_context_slot.unwrap_or_default())
                .unwrap_or_default();
            if minimum_context_slot <= account_data.updated_slot {
                let ui_account =
                    Self::convert_account_data_to_ui_account(&account_data, config.clone());
                Ok((account_data.updated_slot, Some(ui_account)))
            } else {
                Ok((account_data.updated_slot, None))
            }
        } else {
            Err(AccountLoadingError::ConfigDoesnotContainRequiredFilters)
        }
    }

    pub async fn get_program_accounts(
        &self,
        program_id: Pubkey,
        config: Option<RpcProgramAccountsConfig>,
    ) -> anyhow::Result<(Slot, Vec<RpcKeyedAccount>)> {
        GET_PROGRAM_ACCOUNT_CALLED.inc();

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
