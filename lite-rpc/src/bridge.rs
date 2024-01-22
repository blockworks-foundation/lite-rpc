use crate::{
    configs::{IsBlockHashValidConfig, SendTransactionConfig},
    jsonrpsee_subscrption_handler_sink::JsonRpseeSubscriptionHandlerSink,
    rpc::LiteRpcServer,
};
use solana_sdk::epoch_info::EpochInfo;
use std::collections::HashMap;

use solana_lite_rpc_services::{
    transaction_service::TransactionService, tx_sender::TXS_IN_CHANNEL,
};

use anyhow::Context;
use jsonrpsee::{core::SubscriptionResult, server::ServerBuilder, PendingSubscriptionSink};
use prometheus::{opts, register_int_counter, IntCounter};
use solana_lite_rpc_core::{
    encoding,
    stores::{block_information_store::BlockInformation, data_cache::DataCache, tx_store::TxProps},
    AnyhowJoinHandle,
};
use solana_lite_rpc_history::history::History;
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
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, slot_history::Slot};
use solana_transaction_status::{TransactionStatus, UiConfirmedBlock};
use std::{str::FromStr, sync::Arc};
use tokio::net::ToSocketAddrs;

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
}

/// A bridge between clients and tpu
#[allow(dead_code)]
pub struct LiteBridge {
    data_cache: DataCache,
    // should be removed
    rpc_client: Arc<RpcClient>,
    transaction_service: TransactionService,
    history: History,
}

impl LiteBridge {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        data_cache: DataCache,
        transaction_service: TransactionService,
        history: History,
    ) -> Self {
        Self {
            rpc_client,
            data_cache,
            transaction_service,
            history,
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
        todo!()
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

    async fn get_block_height(&self, _config: Option<RpcContextConfig>) -> crate::rpc::Result<u64> {
        todo!()
    }

    async fn get_block_time(&self, _block: u64) -> crate::rpc::Result<u64> {
        todo!()
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
        _pubkey_strs: Option<Vec<String>>,
    ) -> crate::rpc::Result<Vec<RpcPrioritizationFee>> {
        todo!()
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
}
