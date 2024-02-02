use std::collections::HashMap;
use std::{str::FromStr, sync::Arc};

use anyhow::Context;
use jsonrpsee::core::StringError;
use itertools::Itertools;
use jsonrpsee::{
    core::SubscriptionResult, server::ServerBuilder, DisconnectError, PendingSubscriptionSink,
};
use log::{debug, error, warn};
use prometheus::{opts, register_int_counter, IntCounter};
use solana_lite_rpc_prioritization_fees::account_prio_service::AccountPrioService;
use solana_lite_rpc_prioritization_fees::prioritization_fee_calculation_method::PrioritizationFeeCalculationMethod;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::{
    config::{
        RpcBlockSubscribeConfig, RpcBlockSubscribeFilter, RpcBlocksConfigWrapper, RpcContextConfig,
        RpcGetVoteAccountsConfig, RpcLeaderScheduleConfig, RpcProgramAccountsConfig,
        RpcRequestAirdropConfig, RpcSignatureStatusConfig, RpcSignatureSubscribeConfig,
        RpcSignaturesForAddressConfig, RpcTransactionLogsConfig, RpcTransactionLogsFilter,
    },
    response::{
        Response as RpcResponse, RpcBlockhash, RpcConfirmedTransactionStatusWithSignature,
        RpcContactInfo, RpcPerfSample, RpcPrioritizationFee, RpcResponseContext, RpcVersionInfo,
        RpcVoteAccountStatus,
    },
};
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, slot_history::Slot};
use solana_transaction_status::{TransactionStatus, UiConfirmedBlock};
use tokio::net::ToSocketAddrs;
use tokio::sync::broadcast::error::RecvError::{Closed, Lagged};

use solana_lite_rpc_core::{
    encoding,
    stores::{block_information_store::BlockInformation, data_cache::DataCache, tx_store::TxProps},
    AnyhowJoinHandle,
};
use solana_lite_rpc_history::history::History;
use solana_lite_rpc_services::{
    transaction_service::TransactionService, tx_sender::TXS_IN_CHANNEL,
};

use crate::{
    configs::{IsBlockHashValidConfig, SendTransactionConfig},
    jsonrpsee_subscrption_handler_sink::JsonRpseeSubscriptionHandlerSink,
    rpc::LiteRpcServer,
};
use solana_lite_rpc_prioritization_fees::rpc_data::{
    AccountPrioFeesStats, AccountPrioFeesUpdateMessage, PrioFeesStats, PrioFeesUpdateMessage,
};
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
    static ref RPC_SIGNATURE_SUBSCRIBE: IntCounter =
    register_int_counter!(opts!("literpc_rpc_signature_subscribe", "RPC call to subscribe to signature")).unwrap();
    static ref RPC_BLOCK_PRIOFEES_SUBSCRIBE: IntCounter =
    register_int_counter!(opts!("literpc_rpc_block_priofees_subscribe", "RPC call to subscribe to block prio fees")).unwrap();
    static ref RPC_ACCOUNT_PRIOFEES_SUBSCRIBE: IntCounter =
    register_int_counter!(opts!("literpc_rpc_account_priofees_subscribe", "RPC call to subscribe to account prio fees")).unwrap();
}

/// A bridge between clients and tpu
#[allow(dead_code)]
pub struct LiteBridge {
    data_cache: DataCache,
    // should be removed
    rpc_client: Arc<RpcClient>,
    transaction_service: TransactionService,
    history: History,
    prio_fees_service: PrioFeesService,
    account_priofees_service: AccountPrioService,
}

impl LiteBridge {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        data_cache: DataCache,
        transaction_service: TransactionService,
        history: History,
        prio_fees_service: PrioFeesService,
        account_priofees_service: AccountPrioService,
    ) -> Self {
        Self {
            rpc_client,
            data_cache,
            transaction_service,
            history,
            prio_fees_service,
            account_priofees_service,
        }
    }

    /// List for `JsonRpc` requests
    pub async fn start<T: ToSocketAddrs + std::fmt::Debug + 'static + Send + Clone>(
        self,
        http_addr: T,
        ws_addr: T,
    ) -> anyhow::Result<()> {
        let rpc = self.into_rpc();

        let ws_server_handle = ServerBuilder::default()
            .ws_only()
            .build(ws_addr.clone())
            .await?
            .start(rpc.clone())?;

        let http_server_handle = ServerBuilder::default()
            .http_only()
            .build(http_addr.clone())
            .await?
            .start(rpc)?;

        let ws_server: AnyhowJoinHandle = tokio::spawn(async move {
            log::info!("Websocket Server started at {ws_addr:?}");
            ws_server_handle.stopped().await;
            anyhow::bail!("Websocket server stopped");
        });

        let http_server: AnyhowJoinHandle = tokio::spawn(async move {
            log::info!("HTTP Server started at {http_addr:?}");
            http_server_handle.stopped().await;
            anyhow::bail!("HTTP server stopped");
        });

        tokio::select! {
            res = ws_server => {
                anyhow::bail!("WebSocket server {res:?}");
            },
            res = http_server => {
                anyhow::bail!("HTTP server {res:?}");
            },
        }
    }
}

#[jsonrpsee::core::async_trait]
impl LiteRpcServer for LiteBridge {
    async fn get_block(&self, _slot: u64) -> crate::rpc::Result<Option<UiConfirmedBlock>> {
        // let block = self.history.block_storage.query_block(slot).await;
        // if block.is_ok() {
        //     // TO DO Convert to UIConfirmed Block
        //     Err(jsonrpsee::core::Error::HttpNotImplemented)
        // } else {
        //     Ok(None)
        // }

        // TODO get_block might deserve different implementation based on whether we serve from "history module" vs. from "send tx module"
        todo!("get_block: decide where to look")
    }

    async fn get_blocks(
        &self,
        _start_slot: Slot,
        _config: Option<RpcBlocksConfigWrapper>,
        _commitment: Option<CommitmentConfig>,
    ) -> crate::rpc::Result<Vec<Slot>> {
        todo!()
    }

    async fn get_signatures_for_address(
        &self,
        _address: String,
        _config: Option<RpcSignaturesForAddressConfig>,
    ) -> crate::rpc::Result<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        todo!()
    }

    async fn get_cluster_nodes(&self) -> crate::rpc::Result<Vec<RpcContactInfo>> {
        Ok(self.data_cache.cluster_info.cluster_nodes.iter().map(|v| v.value().as_ref().clone()).collect_vec())
    }

    async fn get_slot(&self, config: Option<RpcContextConfig>) -> crate::rpc::Result<Slot> {
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

    async fn get_block_height(&self, config: Option<RpcContextConfig>) -> crate::rpc::Result<u64> {
        let commitment_config = config.map(|x| x.commitment).unwrap_or_default().unwrap_or(CommitmentConfig::finalized());
        let block_info = self.data_cache.block_information_store.get_latest_block(commitment_config).await;
        Ok(block_info.block_height)
    }

    async fn get_block_time(&self, slot: u64) -> crate::rpc::Result<u64> {
        let block_info = self.data_cache.block_information_store.get_block_info_by_slot(slot);
        match block_info {
            Some(info) => Ok(info.block_time),
            None => Err(jsonrpsee::core::Error::Custom("Unable to find block information in LiteRPC cache".to_string())),
        }
    }

    async fn get_first_available_block(&self) -> crate::rpc::Result<u64> {
        todo!()
    }

    async fn get_latest_blockhash(
        &self,
        config: Option<RpcContextConfig>,
    ) -> crate::rpc::Result<RpcResponse<RpcBlockhash>> {
        RPC_GET_LATEST_BLOCKHASH.inc();

        let commitment_config = config
            .map(|config| config.commitment.unwrap_or_default())
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
                blockhash,
                last_valid_block_height: block_height + 150,
            },
        })
    }

    async fn is_blockhash_valid(
        &self,
        blockhash: String,
        config: Option<IsBlockHashValidConfig>,
    ) -> crate::rpc::Result<RpcResponse<bool>> {
        RPC_IS_BLOCKHASH_VALID.inc();

        let commitment = config.unwrap_or_default().commitment.unwrap_or_default();
        let commitment = CommitmentConfig { commitment };

        let (is_valid, slot) = self
            .data_cache
            .block_information_store
            .is_blockhash_valid(&blockhash, commitment)
            .await;

        Ok(RpcResponse {
            context: RpcResponseContext {
                slot,
                api_version: None,
            },
            value: is_valid,
        })
    }

    async fn get_epoch_info(
        &self,
        config: Option<RpcContextConfig>,
    ) -> crate::rpc::Result<EpochInfo> {
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
        _limit: Option<usize>,
    ) -> crate::rpc::Result<Vec<RpcPerfSample>> {
        todo!()
    }

    async fn get_signature_statuses(
        &self,
        sigs: Vec<String>,
        _config: Option<RpcSignatureStatusConfig>,
    ) -> crate::rpc::Result<RpcResponse<Vec<Option<TransactionStatus>>>> {
        RPC_GET_SIGNATURE_STATUSES.inc();

        let sig_statuses = sigs
            .iter()
            .map(|sig| self.data_cache.txs.get(sig).and_then(|v| v.status))
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
        pubkey_strs: Option<Vec<String>>,
    ) -> crate::rpc::Result<Vec<RpcPrioritizationFee>> {
        // This method will get the latest global and account prioritization fee stats and then send the maximum p75
        let accounts = pubkey_strs.map(|pubkeys| pubkeys.iter().filter_map(|pubkey| Pubkey::from_str(&pubkey).ok()).collect_vec()).unwrap_or_default();

        let global_prio_fees = self.prio_fees_service.get_latest_priofees().await;
        let mut max_p75 = global_prio_fees.map(|(_, fees)| {
            let fees = fees.get_percentile(0.75).unwrap_or_default();
            std::cmp::max(fees.0, fees.1)
        }).unwrap_or_default();

        for account in accounts {
            
        }
        max_p75
    }

    async fn send_transaction(
        &self,
        tx: String,
        send_transaction_config: Option<SendTransactionConfig>,
    ) -> crate::rpc::Result<String> {
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
            return Err(jsonrpsee::core::Error::Custom(format!(
                "Transaction too large, expected : {} transaction len {}",
                expected_size,
                tx.len()
            )));
        }

        let raw_tx = match encoding.decode(tx) {
            Ok(raw_tx) => raw_tx,
            Err(err) => {
                return Err(jsonrpsee::core::Error::Custom(err.to_string()));
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
            Err(e) => Err(jsonrpsee::core::Error::Custom(e.to_string())),
        }
    }

    fn get_version(&self) -> crate::rpc::Result<RpcVersionInfo> {
        RPC_GET_VERSION.inc();

        let version = solana_version::Version::default();
        Ok(RpcVersionInfo {
            solana_core: version.to_string(),
            feature_set: Some(version.feature_set),
        })
    }

    async fn request_airdrop(
        &self,
        pubkey_str: String,
        lamports: u64,
        config: Option<RpcRequestAirdropConfig>,
    ) -> crate::rpc::Result<String> {
        RPC_REQUEST_AIRDROP.inc();

        let pubkey = match Pubkey::from_str(&pubkey_str) {
            Ok(pubkey) => pubkey,
            Err(err) => {
                return Err(jsonrpsee::core::Error::Custom(err.to_string()));
            }
        };

        let airdrop_sig = match self
            .rpc_client
            .request_airdrop_with_config(&pubkey, lamports, config.unwrap_or_default())
            .await
            .context("failed to request airdrop")
        {
            Ok(airdrop_sig) => airdrop_sig.to_string(),
            Err(err) => {
                return Err(jsonrpsee::core::Error::Custom(err.to_string()));
            }
        };
        if let Ok((_, block_height)) = self
            .rpc_client
            .get_latest_blockhash_with_commitment(CommitmentConfig::finalized())
            .await
            .context("failed to get latest blockhash")
        {
            self.data_cache.txs.insert(
                airdrop_sig.clone(),
                TxProps {
                    status: None,
                    last_valid_blockheight: block_height,
                    sent_by_lite_rpc: true,
                },
            );
        }
        Ok(airdrop_sig)
    }

    async fn program_subscribe(
        &self,
        _pending: PendingSubscriptionSink,
        _pubkey_str: String,
        _config: Option<RpcProgramAccountsConfig>,
    ) -> SubscriptionResult {
        todo!()
    }

    async fn slot_subscribe(&self, _pending: PendingSubscriptionSink) -> SubscriptionResult {
        todo!()
    }

    async fn block_subscribe(
        &self,
        _pending: PendingSubscriptionSink,
        _filter: RpcBlockSubscribeFilter,
        _config: Option<RpcBlockSubscribeConfig>,
    ) -> SubscriptionResult {
        todo!()
    }

    async fn logs_subscribe(
        &self,
        _pending: PendingSubscriptionSink,
        _filter: RpcTransactionLogsFilter,
        _config: Option<RpcTransactionLogsConfig>,
    ) -> SubscriptionResult {
        todo!()
    }

    // WARN: enable_received_notification: bool is ignored
    async fn signature_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        signature: String,
        config: RpcSignatureSubscribeConfig,
    ) -> SubscriptionResult {
        RPC_SIGNATURE_SUBSCRIBE.inc();
        let sink = pending.accept().await?;

        let jsonrpsee_sink = JsonRpseeSubscriptionHandlerSink::new(sink);
        self.data_cache.tx_subs.signature_subscribe(
            signature,
            config.commitment.unwrap_or_default(),
            Arc::new(jsonrpsee_sink),
        );

        Ok(())
    }

    async fn slot_updates_subscribe(
        &self,
        _pending: PendingSubscriptionSink,
    ) -> SubscriptionResult {
        todo!()
    }

    async fn vote_subscribe(&self, _pending: PendingSubscriptionSink) -> SubscriptionResult {
        todo!()
    }

    async fn get_leader_schedule(
        &self,
        slot: Option<u64>,
        config: Option<RpcLeaderScheduleConfig>,
    ) -> crate::rpc::Result<Option<HashMap<String, Vec<usize>>>> {
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
    async fn get_slot_leaders(
        &self,
        start_slot: u64,
        limit: u64,
    ) -> crate::rpc::Result<Vec<Pubkey>> {
        let epock_schedule = self.data_cache.epoch_data.get_epoch_schedule();

        self.data_cache
            .leader_schedule
            .read()
            .await
            .get_slot_leaders(start_slot, limit, epock_schedule)
            .await
            .map_err(|err| {
                jsonrpsee::core::Error::Custom(format!("error during query processing:{err}"))
            })
    }

    async fn get_vote_accounts(
        &self,
        _config: Option<RpcGetVoteAccountsConfig>,
    ) -> crate::rpc::Result<RpcVoteAccountStatus> {
        todo!()
    }

    async fn get_latest_block_priofees(
        &self,
        method: Option<PrioritizationFeeCalculationMethod>,
    ) -> crate::rpc::Result<RpcResponse<PrioFeesStats>> {
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
                return Err(jsonrpsee::core::Error::Custom(
                    "Invalid calculation method".to_string(),
                ))
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
            None => Err(jsonrpsee::core::Error::Custom(
                "No latest priofees stats available found".to_string(),
            )),
        }
    }

    // use websocket-tungstenite-retry->examples/consume_literpc_priofees.rs to test
    async fn latest_block_priofees_subscribe(
        &self,
        pending: PendingSubscriptionSink,
    ) -> SubscriptionResult {
        let sink = pending.accept().await?;

        let mut block_fees_stream = self.prio_fees_service.block_fees_stream.subscribe();
        tokio::spawn(async move {
            RPC_BLOCK_PRIOFEES_SUBSCRIBE.inc();

            'recv_loop: loop {
                match block_fees_stream.recv().await {
                    Ok(PrioFeesUpdateMessage {
                        slot: confirmation_slot,
                        priofees_stats,
                    }) => {
                        let result_message =
                            jsonrpsee::SubscriptionMessage::from_json(&RpcResponse {
                                context: RpcResponseContext {
                                    slot: confirmation_slot,
                                    api_version: None,
                                },
                                value: priofees_stats,
                            });

                        match sink.send(result_message.unwrap()).await {
                            Ok(()) => {
                                // success
                                continue 'recv_loop;
                            }
                            Err(DisconnectError(_subscription_message)) => {
                                debug!("Stopping subscription task on disconnect");
                                return;
                            }
                        };
                    }
                    Err(Lagged(lagged)) => {
                        // this usually happens if there is one "slow receiver", see https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html#lagging
                        warn!(
                            "subscriber laggs some({}) priofees update messages - continue",
                            lagged
                        );
                        continue 'recv_loop;
                    }
                    Err(Closed) => {
                        error!("failed to receive block, sender closed - aborting");
                        return;
                    }
                }
            }
        });

        Ok(())
    }

    async fn get_latest_account_priofees(
        &self,
        account: String,
        method: Option<PrioritizationFeeCalculationMethod>,
    ) -> crate::rpc::Result<RpcResponse<AccountPrioFeesStats>> {
        if let Ok(account) = Pubkey::from_str(&account) {
            let method = method.unwrap_or_default();
            let (slot, value) = match method {
                PrioritizationFeeCalculationMethod::Latest => {
                    self.account_priofees_service.get_latest_stats(&account)
                }
                PrioritizationFeeCalculationMethod::LastNBlocks(nb) => {
                    self.account_priofees_service.get_n_last_stats(&account, nb)
                }
                _ => {
                    return Err(jsonrpsee::core::Error::Custom(
                        "Invalid calculation method".to_string(),
                    ))
                }
            };
            Ok(RpcResponse {
                context: RpcResponseContext {
                    slot,
                    api_version: None,
                },
                value,
            })
        } else {
            Err(jsonrpsee::core::Error::Custom(
                "Invalid account".to_string(),
            ))
        }
    }

    async fn latest_account_priofees_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        account: String,
    ) -> SubscriptionResult {
        let Ok(account) = Pubkey::from_str(&account) else {
            return Err(StringError::from("Invalid account".to_string()));
        };
        let sink = pending.accept().await?;
        let mut account_fees_stream = self
            .account_priofees_service
            .priofees_update_sender
            .subscribe();
        tokio::spawn(async move {
            RPC_BLOCK_PRIOFEES_SUBSCRIBE.inc();

            'recv_loop: loop {
                match account_fees_stream.recv().await {
                    Ok(AccountPrioFeesUpdateMessage {
                        slot,
                        accounts_data,
                    }) => {
                        if let Some(account_data) = accounts_data.get(&account) {
                            let result_message =
                                jsonrpsee::SubscriptionMessage::from_json(&RpcResponse {
                                    context: RpcResponseContext {
                                        slot,
                                        api_version: None,
                                    },
                                    value: account_data,
                                });

                            match sink.send(result_message.unwrap()).await {
                                Ok(()) => {
                                    // success
                                    continue 'recv_loop;
                                }
                                Err(DisconnectError(_subscription_message)) => {
                                    debug!("Stopping subscription task on disconnect");
                                    return;
                                }
                            };
                        }
                    }
                    Err(Lagged(lagged)) => {
                        // this usually happens if there is one "slow receiver", see https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html#lagging
                        warn!(
                            "subscriber laggs some({}) priofees update messages - continue",
                            lagged
                        );
                        continue 'recv_loop;
                    }
                    Err(Closed) => {
                        error!("failed to receive block, sender closed - aborting");
                        return;
                    }
                }
            }
        });

        Ok(())
    }
}
