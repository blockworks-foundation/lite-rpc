use std::{str::FromStr, sync::Arc, time::Duration};

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::json;
use solana_account_decoder::UiDataSliceConfig;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_response::OptionalContext,
};
use solana_lite_rpc_core::{
    encoding::BASE64,
    structures::{
        account_data::{Account, AccountData, CompressionMethod},
        account_filter::AccountFilters,
    },
};
use solana_sdk::{
    account::{Account as SolanaAccount, AccountSharedData, ReadableAccount},
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
};

use crate::account_store_interface::AccountStorageInterface;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RpcKeyedCompressedAccount {
    pub p: String,
    pub a: String,
}

pub async fn get_program_account(
    rpc_url: String,
    filters: &AccountFilters,
    max_request_in_parallel: usize,
    number_of_retires: usize,
    number_of_accounts_in_gma: usize,
    account_store: Arc<dyn AccountStorageInterface>,
) -> anyhow::Result<()> {
    // setting larget timeout because gPA can take a lot of time
    let rpc_client = Arc::new(RpcClient::new_with_timeout_and_commitment(
        rpc_url,
        Duration::from_secs(60 * 20),
        CommitmentConfig::processed(),
    ));

    // use getGPA compressed if available
    let mut accounts = vec![];
    {
        for filter in filters.iter() {
            if !filter.accounts.is_empty() {
                accounts.extend(filter.accounts.clone());
            }

            if let Some(program_id) = &filter.program_id {
                log::info!("gPA for {}", program_id.to_string());

                let result = rpc_client
                    .send::<OptionalContext<Vec<RpcKeyedCompressedAccount>>>(
                        solana_client::rpc_request::RpcRequest::Custom {
                            method: "getProgramAccountsCompressed",
                        },
                        json!([
                            program_id.to_string(),
                            RpcProgramAccountsConfig {
                                filters: filter.get_rpc_filter(),
                                with_context: Some(true),
                                ..Default::default()
                            }
                        ]),
                    )
                    .await;

                // failed to get over compressed program accounts
                match result {
                    Ok(OptionalContext::Context(response)) => {
                        log::info!("Received compressed data for {}", program_id);
                        let updated_slot = response.context.slot;

                        for key_account in response.value {
                            let base64_decoded = BASE64.decode(&key_account.a)?;
                            // decompress all the account information
                            let uncompressed = lz4::block::decompress(&base64_decoded, None)?;
                            let shared_data =
                                bincode::deserialize::<AccountSharedData>(&uncompressed)?;
                            let account = SolanaAccount {
                                lamports: shared_data.lamports(),
                                data: shared_data.data().to_vec(),
                                owner: *shared_data.owner(),
                                executable: shared_data.executable(),
                                rent_epoch: shared_data.rent_epoch(),
                            };

                            // compress just account_data

                            account_store
                                .initilize_or_update_account(AccountData {
                                    pubkey: Pubkey::from_str(&key_account.p)?,
                                    account: Arc::new(Account::from_solana_account(
                                        account,
                                        CompressionMethod::Lz4(1),
                                    )),
                                    updated_slot,
                                    write_version: 0,
                                })
                                .await;
                        }
                    }
                    _ => {
                        // getProgramAccountCompressed not available, using gPA instead
                        log::info!("fallback to gPA for {}", program_id);
                        let mut rpc_acc = rpc_client
                            .get_program_accounts_with_config(
                                program_id,
                                RpcProgramAccountsConfig {
                                    filters: filter.get_rpc_filter(),
                                    account_config: RpcAccountInfoConfig {
                                        encoding: Some(
                                            solana_account_decoder::UiAccountEncoding::Base64,
                                        ),
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
            }
        }
    }

    log::info!("Fetching {} accounts", accounts.len());
    for accounts in accounts.chunks(max_request_in_parallel * number_of_accounts_in_gma) {
        for accounts in accounts.chunks(number_of_accounts_in_gma) {
            let mut fetch_accounts = vec![];
            let mut updated_slot = 0;
            for _ in 0..number_of_retires {
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
            for (index, account) in fetch_accounts.drain(0..).enumerate() {
                if let Some(account) = account {
                    account_store
                        .initilize_or_update_account(AccountData {
                            pubkey: accounts[index],
                            account: Arc::new(Account::from_solana_account(
                                account,
                                CompressionMethod::Lz4(1),
                            )),
                            updated_slot,
                            write_version: 0,
                        })
                        .await;
                }
            }
        }
    }
    log::info!("{} accounts successfully fetched", accounts.len());
    Ok(())
}
