use itertools::Itertools;
use jsonrpsee::core::RpcResult;
use prometheus::{opts, register_int_counter, IntCounter};
use solana_account_decoder::UiAccount;
use solana_lite_rpc_accounts::account_service::AccountService;
use solana_lite_rpc_prioritization_fees::account_prio_service::AccountPrioService;
use solana_lite_rpc_prioritization_fees::prioritization_fee_calculation_method::PrioritizationFeeCalculationMethod;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::RpcAccountInfoConfig;
use solana_rpc_client_api::response::{OptionalContext, RpcKeyedAccount};
use solana_rpc_client_api::{
    config::{
        RpcBlocksConfigWrapper, RpcContextConfig, RpcGetVoteAccountsConfig,
        RpcLeaderScheduleConfig, RpcProgramAccountsConfig, RpcRequestAirdropConfig,
        RpcSignatureStatusConfig, RpcSignaturesForAddressConfig,
    },
    response::{
        Response as RpcResponse, RpcBlockhash, RpcConfirmedTransactionStatusWithSignature,
        RpcContactInfo, RpcPerfSample, RpcPrioritizationFee, RpcResponseContext, RpcVersionInfo,
        RpcVoteAccountStatus,
    },
};
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::signature::Signature;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, slot_history::Slot};
use solana_transaction_status::{TransactionStatus, UiConfirmedBlock};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use solana_lite_rpc_blockstore::history::History;
use solana_lite_rpc_core::solana_utils::hash_from_str;
use solana_lite_rpc_core::{
    encoding,
    stores::{block_information_store::BlockInformation, data_cache::DataCache},
};
use solana_lite_rpc_services::{
    transaction_service::TransactionService, tx_sender::TXS_IN_CHANNEL,
};

use crate::rpc_errors::RpcErrors;
use crate::{
    configs::{IsBlockHashValidConfig, SendTransactionConfig},
    rpc::LiteRpcServer,
};
use solana_lite_rpc_prioritization_fees::rpc_data::{AccountPrioFeesStats, PrioFeesStats};
use solana_lite_rpc_prioritization_fees::PrioFeesService;

lazy_static::lazy_static! {
    static ref RPC_SEND_TX: IntCounter =
    register_int_counter!(opts!("literpc_rpc_send_tx", "RPC call send transaction")).unwrap();
    static ref RPC_GET_LATEST_BLOCKHASH: IntCounter =
    register_int_counter!(opts!("literpc_rpc_get_latest_blockhash", "RPC call to get latest block hash")).unwrap();
    static ref RPC_IS_BLOCKHASH_VALID: IntCounter =
    register_int_counter!(opts!("literpc_rpc_is_blockhash_valid", "RPC call to check if blockhash is vali calld")).unwrap();
    static ref RPC_GET_SIGNATURE_STATUSES: IntCounter =
    register_int_counter!(opts!("literpc_rpc_get_signature_statuses", "RPC call to get signature statuses")).unwrap();
    static ref RPC_GET_VERSION: IntCounter =
    register_int_counter!(opts!("literpc_rpc_get_version", "RPC call to version")).unwrap();
    static ref RPC_REQUEST_AIRDROP: IntCounter =
    register_int_counter!(opts!("literpc_rpc_airdrop", "RPC call to request airdrop")).unwrap();
}

/// A bridge between clients and tpu
#[allow(dead_code)]
pub struct LiteBridge {
    rpc_client: Arc<RpcClient>,
    data_cache: DataCache,
    transaction_service: TransactionService,
    history: History,
    prio_fees_service: PrioFeesService,
    account_priofees_service: AccountPrioService,
    accounts_service: Option<AccountService>,
}

impl LiteBridge {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        data_cache: DataCache,
        transaction_service: TransactionService,
        history: History,
        prio_fees_service: PrioFeesService,
        account_priofees_service: AccountPrioService,
        accounts_service: Option<AccountService>,
    ) -> Self {
        Self {
            rpc_client,
            data_cache,
            transaction_service,
            history,
            prio_fees_service,
            account_priofees_service,
            accounts_service,
        }
    }
}

#[jsonrpsee::core::async_trait]
impl LiteRpcServer for LiteBridge {
    async fn get_block(&self, _slot: u64) -> RpcResult<Option<UiConfirmedBlock>> {
        // let block = self.blockstore.block_storage.query_block(slot).await;
        // if block.is_ok() {
        //     // TO DO Convert to UIConfirmed Block
        //     Err(jsonrpsee::core::Error::HttpNotImplemented)
        // } else {
        //     Ok(None)
        // }

        // TODO get_block might deserve different implementation based on whether we serve from "blockstore module" vs. from "send tx module"

        // under progress
        Err(jsonrpsee::types::error::ErrorCode::MethodNotFound.into())
    }

    async fn get_blocks(
        &self,
        _start_slot: Slot,
        _config: Option<RpcBlocksConfigWrapper>,
        _commitment: Option<CommitmentConfig>,
    ) -> RpcResult<Vec<Slot>> {
        // under progress
        Err(jsonrpsee::types::error::ErrorCode::MethodNotFound.into())
    }

    async fn get_signatures_for_address(
        &self,
        _address: String,
        _config: Option<RpcSignaturesForAddressConfig>,
    ) -> RpcResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        // under progress
        Err(jsonrpsee::types::error::ErrorCode::MethodNotFound.into())
    }

    async fn get_cluster_nodes(&self) -> RpcResult<Vec<RpcContactInfo>> {
        Ok(self
            .data_cache
            .cluster_info
            .cluster_nodes
            .iter()
            .map(|v| v.value().as_ref().clone())
            .collect_vec())
    }

    async fn get_slot(&self, config: Option<RpcContextConfig>) -> RpcResult<Slot> {
        let commitment_config = config
            .map(|config| config.commitment.unwrap_or_default())
            .unwrap_or_default();

        let BlockInformation { slot, .. } = self
            .data_cache
            .block_information_store
            .get_latest_block(commitment_config)
            .await;
        Ok(slot)
    }

    async fn get_block_height(&self, config: Option<RpcContextConfig>) -> RpcResult<u64> {
        let commitment_config = config.map_or(CommitmentConfig::finalized(), |x| {
            x.commitment.unwrap_or_default()
        });
        let block_info = self
            .data_cache
            .block_information_store
            .get_latest_block(commitment_config)
            .await;
        Ok(block_info.block_height)
    }

    async fn get_block_time(&self, slot: u64) -> RpcResult<u64> {
        let block_info = self
            .data_cache
            .block_information_store
            .get_block_info_by_slot(slot);
        match block_info {
            Some(info) => Ok(info.block_time),
            None => Err(jsonrpsee::types::error::ErrorCode::InvalidParams.into()),
        }
    }

    async fn get_first_available_block(&self) -> RpcResult<u64> {
        // under progress
        Err(jsonrpsee::types::error::ErrorCode::MethodNotFound.into())
    }

    async fn get_latest_blockhash(
        &self,
        config: Option<RpcContextConfig>,
    ) -> RpcResult<RpcResponse<RpcBlockhash>> {
        RPC_GET_LATEST_BLOCKHASH.inc();

        let commitment_config = config
            .map(|config| config.commitment.unwrap_or(CommitmentConfig::confirmed()))
            .unwrap_or_default();

        let BlockInformation {
            slot,
            block_height,
            blockhash,
            ..
        } = self
            .data_cache
            .block_information_store
            .get_latest_block(commitment_config)
            .await;

        log::trace!("glb {blockhash} {slot} {block_height}");

        Ok(RpcResponse {
            context: RpcResponseContext {
                slot,
                api_version: None,
            },
            value: RpcBlockhash {
                blockhash: blockhash.to_string(),
                last_valid_block_height: block_height + 150,
            },
        })
    }

    async fn is_blockhash_valid(
        &self,
        blockhash: String,
        config: Option<IsBlockHashValidConfig>,
    ) -> RpcResult<RpcResponse<bool>> {
        RPC_IS_BLOCKHASH_VALID.inc();

        let commitment = config.unwrap_or_default().commitment.unwrap_or_default();
        let commitment = CommitmentConfig { commitment };

        let (is_valid, slot) = self
            .data_cache
            .block_information_store
            .is_blockhash_valid(
                &hash_from_str(&blockhash).expect("valid blockhash"),
                commitment,
            )
            .await;

        Ok(RpcResponse {
            context: RpcResponseContext {
                slot,
                api_version: None,
            },
            value: is_valid,
        })
    }

    async fn get_epoch_info(&self, config: Option<RpcContextConfig>) -> RpcResult<EpochInfo> {
        let commitment_config = config
            .map(|config| config.commitment.unwrap_or_default())
            .unwrap_or_default();
        let block_info = self
            .data_cache
            .block_information_store
            .get_latest_block_info(commitment_config)
            .await;

        //TODO manage transaction_count of epoch info. Currently None.
        let epoch_info = self
            .data_cache
            .get_current_epoch(commitment_config)
            .await
            .as_epoch_info(block_info.block_height, None);
        Ok(epoch_info)
    }

    async fn get_recent_performance_samples(
        &self,
        limit: Option<usize>,
    ) -> RpcResult<Vec<RpcPerfSample>> {
        // TODO: implement our own perofmrance samples from blockstream and slot stream
        // For now just use normal rpc to get the data
        self.rpc_client
            .get_recent_performance_samples(limit)
            .await
            .map_err(|_| jsonrpsee::types::error::ErrorCode::InternalError.into())
    }

    async fn get_signature_statuses(
        &self,
        sigs: Vec<String>,
        _config: Option<RpcSignatureStatusConfig>,
    ) -> RpcResult<RpcResponse<Vec<Option<TransactionStatus>>>> {
        RPC_GET_SIGNATURE_STATUSES.inc();

        let sig_statuses = sigs
            .iter()
            .map(|sig| Signature::from_str(sig).expect("signature must be valid"))
            .map(|sig| self.data_cache.txs.get(&sig).and_then(|v| v.status))
            .collect();

        Ok(RpcResponse {
            context: RpcResponseContext {
                slot: self
                    .data_cache
                    .block_information_store
                    .get_latest_block_info(CommitmentConfig::finalized())
                    .await
                    .slot,
                api_version: None,
            },
            value: sig_statuses,
        })
    }

    async fn get_recent_prioritization_fees(
        &self,
        pubkey_strs: Vec<String>,
    ) -> RpcResult<Vec<RpcPrioritizationFee>> {
        // This method will get the latest global and account prioritization fee stats and then send the maximum p75
        const PERCENTILE: f32 = 0.75;
        let accounts = pubkey_strs
            .iter()
            .filter_map(|pubkey| Pubkey::from_str(pubkey).ok())
            .collect_vec();
        if accounts.len() != pubkey_strs.len() {
            // if lengths do not match it means some of the accounts are invalid
            return Err(jsonrpsee::types::error::ErrorCode::InvalidParams.into());
        }

        let global_prio_fees = self.prio_fees_service.get_latest_priofees().await;
        let max_p75 = global_prio_fees
            .map(|(_, fees)| {
                let fees = fees.get_percentile(PERCENTILE).unwrap_or_default();
                std::cmp::max(fees.0, fees.1)
            })
            .unwrap_or_default();

        let ret: Vec<RpcPrioritizationFee> = accounts
            .iter()
            .map(|account| {
                let (slot, stats) = self.account_priofees_service.get_latest_stats(account);
                let stat = stats
                    .all_stats
                    .get_percentile(PERCENTILE)
                    .unwrap_or_default();
                RpcPrioritizationFee {
                    slot,
                    prioritization_fee: std::cmp::max(max_p75, std::cmp::max(stat.0, stat.1)),
                }
            })
            .collect_vec();

        Ok(ret)
    }

    async fn send_transaction(
        &self,
        tx: String,
        send_transaction_config: Option<SendTransactionConfig>,
    ) -> RpcResult<String> {
        RPC_SEND_TX.inc();

        // Copied these constants from solana labs code
        const MAX_BASE58_SIZE: usize = 1683;
        const MAX_BASE64_SIZE: usize = 1644;

        let SendTransactionConfig {
            encoding,
            max_retries,
        } = send_transaction_config.unwrap_or_default();

        let expected_size = match encoding {
            encoding::BinaryEncoding::Base58 => MAX_BASE58_SIZE,
            encoding::BinaryEncoding::Base64 => MAX_BASE64_SIZE,
        };
        if tx.len() > expected_size {
            return Err(jsonrpsee::types::error::ErrorCode::OversizedRequest.into());
        }

        let raw_tx = match encoding.decode(tx) {
            Ok(raw_tx) => raw_tx,
            Err(_) => {
                return Err(jsonrpsee::types::error::ErrorCode::InvalidParams.into());
            }
        };

        match self
            .transaction_service
            .send_transaction(raw_tx, max_retries)
            .await
        {
            Ok(sig) => {
                TXS_IN_CHANNEL.inc();

                Ok(sig)
            }
            Err(_) => Err(jsonrpsee::types::error::ErrorCode::InternalError.into()),
        }
    }

    fn get_version(&self) -> RpcResult<RpcVersionInfo> {
        RPC_GET_VERSION.inc();

        let version = solana_version::Version::default();
        Ok(RpcVersionInfo {
            solana_core: version.to_string(),
            feature_set: Some(version.feature_set),
        })
    }

    async fn request_airdrop(
        &self,
        _pubkey_str: String,
        _lamports: u64,
        _config: Option<RpcRequestAirdropConfig>,
    ) -> RpcResult<String> {
        RPC_REQUEST_AIRDROP.inc();
        Err(jsonrpsee::types::error::ErrorCode::MethodNotFound.into())
    }

    async fn get_leader_schedule(
        &self,
        slot: Option<u64>,
        config: Option<RpcLeaderScheduleConfig>,
    ) -> RpcResult<Option<HashMap<String, Vec<usize>>>> {
        //TODO verify leader identity.
        let schedule = self
            .data_cache
            .leader_schedule
            .read()
            .await
            .get_leader_schedule_for_slot(slot, config.and_then(|c| c.commitment), &self.data_cache)
            .await;
        Ok(schedule)
    }

    async fn get_slot_leaders(&self, start_slot: u64, limit: u64) -> RpcResult<Vec<Pubkey>> {
        let epock_schedule = self.data_cache.epoch_data.get_epoch_schedule();

        self.data_cache
            .leader_schedule
            .read()
            .await
            .get_slot_leaders(start_slot, limit, epock_schedule)
            .await
            .map_err(|err| {
                log::error!("Error processing get leader schedule : {err:?}");
                jsonrpsee::types::error::ErrorCode::InternalError.into()
            })
    }

    async fn get_vote_accounts(
        &self,
        _config: Option<RpcGetVoteAccountsConfig>,
    ) -> RpcResult<RpcVoteAccountStatus> {
        // under progress
        Err(jsonrpsee::types::error::ErrorCode::MethodNotFound.into())
    }

    async fn get_latest_block_priofees(
        &self,
        method: Option<PrioritizationFeeCalculationMethod>,
    ) -> RpcResult<RpcResponse<PrioFeesStats>> {
        let method = method.unwrap_or_default();
        let res = match method {
            PrioritizationFeeCalculationMethod::Latest => {
                self.prio_fees_service.get_latest_priofees().await
            }
            PrioritizationFeeCalculationMethod::LastNBlocks(nb) => {
                self.prio_fees_service
                    .get_last_n_priofees_aggregate(nb)
                    .await
            }
            _ => {
                // method is invalid
                return Err(jsonrpsee::types::error::ErrorCode::InvalidParams.into());
            }
        };

        match res {
            Some((confirmation_slot, priofees)) => Ok(RpcResponse {
                context: RpcResponseContext {
                    slot: confirmation_slot,
                    api_version: None,
                },
                value: priofees,
            }),
            None => Err(jsonrpsee::types::error::ErrorCode::InternalError.into()),
        }
    }

    async fn get_latest_account_priofees(
        &self,
        account: String,
        method: Option<PrioritizationFeeCalculationMethod>,
    ) -> RpcResult<RpcResponse<AccountPrioFeesStats>> {
        if let Ok(account) = Pubkey::from_str(&account) {
            let method = method.unwrap_or_default();
            let (slot, value) = match method {
                PrioritizationFeeCalculationMethod::Latest => {
                    self.account_priofees_service.get_latest_stats(&account)
                }
                PrioritizationFeeCalculationMethod::LastNBlocks(nb) => {
                    self.account_priofees_service.get_n_last_stats(&account, nb)
                }
                _ => return Err(jsonrpsee::types::error::ErrorCode::InternalError.into()),
            };
            Ok(RpcResponse {
                context: RpcResponseContext {
                    slot,
                    api_version: None,
                },
                value,
            })
        } else {
            // Account key is invalid
            Err(jsonrpsee::types::error::ErrorCode::InvalidParams.into())
        }
    }

    async fn get_account_info(
        &self,
        pubkey_str: String,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Option<UiAccount>>> {
        let Ok(pubkey) = Pubkey::from_str(&pubkey_str) else {
            // pubkey is invalid
            return Err(jsonrpsee::types::error::ErrorCode::InvalidParams.into());
        };
        if let Some(account_service) = &self.accounts_service {
            let commitment = config
                .as_ref()
                .and_then(|x| x.commitment)
                .unwrap_or_default();
            let current_block_info = self
                .data_cache
                .block_information_store
                .get_latest_block_info(commitment)
                .await;
            match account_service.get_account(pubkey, config).await {
                Ok((_, ui_account)) => Ok(RpcResponse {
                    context: RpcResponseContext {
                        slot: current_block_info.slot,
                        api_version: None,
                    },
                    value: ui_account,
                }),
                Err(_) => {
                    // account not found
                    Err(jsonrpsee::types::error::ErrorCode::ServerError(
                        RpcErrors::AccountNotFound as i32,
                    )
                    .into())
                }
            }
        } else {
            // accounts are disabled
            Err(jsonrpsee::types::error::ErrorCode::MethodNotFound.into())
        }
    }

    async fn get_multiple_accounts(
        &self,
        pubkey_strs: Vec<String>,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Vec<Option<UiAccount>>>> {
        let pubkeys = pubkey_strs
            .iter()
            .map(|key| Pubkey::from_str(key))
            .collect_vec();
        if pubkeys.iter().any(|res| res.is_err()) {
            return Err(jsonrpsee::types::error::ErrorCode::InternalError.into());
        };

        if let Some(account_service) = &self.accounts_service {
            let mut ui_accounts = vec![];
            for pubkey in pubkeys {
                match account_service
                    .get_account(pubkey.unwrap(), config.clone())
                    .await
                {
                    Ok((_, ui_account)) => {
                        ui_accounts.push(ui_account);
                    }
                    Err(_) => {
                        ui_accounts.push(None);
                    }
                }
            }
            let commitment = config
                .as_ref()
                .and_then(|x| x.commitment)
                .unwrap_or_default();
            let current_block_info = self
                .data_cache
                .block_information_store
                .get_latest_block_info(commitment)
                .await;
            assert_eq!(ui_accounts.len(), pubkey_strs.len());
            Ok(RpcResponse {
                context: RpcResponseContext {
                    slot: current_block_info.slot,
                    api_version: None,
                },
                value: ui_accounts,
            })
        } else {
            // accounts are disabled
            Err(jsonrpsee::types::error::ErrorCode::MethodNotFound.into())
        }
    }

    async fn get_program_accounts(
        &self,
        program_id_str: String,
        config: Option<RpcProgramAccountsConfig>,
    ) -> RpcResult<OptionalContext<Vec<RpcKeyedAccount>>> {
        let Ok(program_id) = Pubkey::from_str(&program_id_str) else {
            return Err(jsonrpsee::types::error::ErrorCode::InternalError.into());
        };
        let with_context = config
            .as_ref()
            .map(|value| value.with_context.unwrap_or_default())
            .unwrap_or_default();

        let commitment: CommitmentConfig = config
            .as_ref()
            .and_then(|x| x.account_config.commitment)
            .unwrap_or_default();
        let current_block_info = self
            .data_cache
            .block_information_store
            .get_latest_block_info(commitment)
            .await;

        if let Some(account_service) = &self.accounts_service {
            match account_service
                .get_program_accounts(program_id, config)
                .await
            {
                Ok((_, ui_account)) => {
                    if with_context {
                        Ok(OptionalContext::Context(RpcResponse {
                            context: RpcResponseContext {
                                slot: current_block_info.slot,
                                api_version: None,
                            },
                            value: ui_account,
                        }))
                    } else {
                        Ok(OptionalContext::NoContext(ui_account))
                    }
                }
                Err(_) => {
                    return Err(jsonrpsee::types::error::ErrorCode::ServerError(
                        RpcErrors::AccountNotFound as i32,
                    )
                    .into());
                }
            }
        } else {
            // accounts are disabled
            Err(jsonrpsee::types::error::ErrorCode::MethodNotFound.into())
        }
    }

    async fn get_balance(
        &self,
        pubkey_str: String,
        config: Option<RpcContextConfig>,
    ) -> RpcResult<RpcResponse<u64>> {
        let Ok(pubkey) = Pubkey::from_str(&pubkey_str) else {
            // pubkey is invalid
            return Err(jsonrpsee::types::error::ErrorCode::InvalidParams.into());
        };
        let config = config.map(|x| RpcAccountInfoConfig {
            encoding: None,
            data_slice: None,
            commitment: x.commitment,
            min_context_slot: x.min_context_slot,
        });

        let commitment = config
            .as_ref()
            .and_then(|x| x.commitment)
            .unwrap_or_default();
        let current_block_info = self
            .data_cache
            .block_information_store
            .get_latest_block_info(commitment)
            .await;

        if let Some(account_service) = &self.accounts_service {
            match account_service.get_account(pubkey, config).await {
                Ok((_, ui_account)) => Ok(RpcResponse {
                    context: RpcResponseContext {
                        slot: current_block_info.slot,
                        api_version: None,
                    },
                    value: ui_account.map(|x| x.lamports).unwrap_or_default(),
                }),
                Err(_) => {
                    // account not found
                    Err(jsonrpsee::types::error::ErrorCode::ServerError(
                        RpcErrors::AccountNotFound as i32,
                    )
                    .into())
                }
            }
        } else {
            // accounts are disabled
            Err(jsonrpsee::types::error::ErrorCode::MethodNotFound.into())
        }
    }
}
