use crate::{
    configs::{IsBlockHashValidConfig, SendTransactionConfig},
    jsonrpsee_subscrption_handler_sink::JsonRpseeSubscriptionHandlerSink,
    rpc::LiteRpcServer,
};
use solana_sdk::epoch_info::EpochInfo;
use solana_lite_rpc_services::{
    transaction_service::TransactionService, tx_sender::TXS_IN_CHANNEL,
};
use anyhow::Context;
use jsonrpsee::{core::SubscriptionResult, server::ServerBuilder, PendingSubscriptionSink};
use log::info;
use prometheus::{opts, register_int_counter, IntCounter};
use solana_lite_rpc_core::{
    stores::{block_information_store::BlockInformation, data_cache::DataCache, tx_store::TxProps},
    AnyhowJoinHandle,
};
use solana_lite_rpc_history::history::History;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::{
    config::{
        RpcBlockConfig, RpcContextConfig, RpcEncodingConfigWrapper, RpcRequestAirdropConfig,
        RpcSignatureStatusConfig,
    },
    response::{Response as RpcResponse, RpcBlockhash, RpcResponseContext, RpcVersionInfo},
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, slot_history::Slot};
use solana_transaction_status::{TransactionStatus, UiConfirmedBlock, EncodedTransactionWithStatusMeta, UiTransactionStatusMeta, option_serializer::OptionSerializer};
use std::{str::FromStr, sync::Arc};
use tokio::net::ToSocketAddrs;
use itertools::Itertools;

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
    async fn send_transaction(
        &self,
        tx: String,
        send_transaction_config: Option<SendTransactionConfig>,
    ) -> crate::rpc::Result<String> {
        RPC_SEND_TX.inc();

        let SendTransactionConfig {
            encoding,
            max_retries,
        } = send_transaction_config.unwrap_or_default();

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

        info!("glb {blockhash} {slot} {block_height}");

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

    async fn signature_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        signature: String,
        commitment_config: CommitmentConfig,
    ) -> SubscriptionResult {
        RPC_SIGNATURE_SUBSCRIBE.inc();
        let sink = pending.accept().await?;

        let jsonrpsee_sink = JsonRpseeSubscriptionHandlerSink::new(sink);
        self.data_cache.tx_subs.signature_subscribe(
            signature,
            commitment_config,
            Arc::new(jsonrpsee_sink),
        );

        Ok(())
    }

    async fn get_block(
        &self,
        slot: u64,
        config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
    ) -> crate::rpc::Result<Option<UiConfirmedBlock>> {
        let config = config.map_or(RpcBlockConfig::default(), |x| x.convert_to_current());
        let block = self.history.block_storage.get(slot, config).await;
        if let Ok(block) = block {

            let transactions: Option<Vec<EncodedTransactionWithStatusMeta>> = match config.transaction_details {
                Some(transaction_details) => {
                    let (is_full, include_rewards, include_accounts) = match transaction_details {
                        solana_transaction_status::TransactionDetails::Full => (true, true, true),
                        solana_transaction_status::TransactionDetails::Signatures => ( false, false, false),
                        solana_transaction_status::TransactionDetails::None => (false, false, false),
                        solana_transaction_status::TransactionDetails::Accounts => (false, false, true),
                    };

                    if is_full || include_accounts || include_rewards {
                        Some(block.transactions.iter().map(|transaction_info| {
                            EncodedTransactionWithStatusMeta {
                                version: None,
                                meta: Some(UiTransactionStatusMeta {
                                    err: transaction_info.err.clone(),
                                    status: transaction_info.err.clone().map_or(Ok(()), |x| Err(x)),
                                    fee: 0,
                                    pre_balances: vec![],
                                    post_balances: vec![],
                                    inner_instructions: OptionSerializer::None,
                                    log_messages: OptionSerializer::None,
                                    pre_token_balances: OptionSerializer::None,
                                    post_token_balances: OptionSerializer::None,
                                    rewards: OptionSerializer::None,
                                    loaded_addresses: OptionSerializer::None,
                                    return_data: OptionSerializer::None,
                                    compute_units_consumed: transaction_info.cu_consumed.map_or( OptionSerializer::None, |x| OptionSerializer::Some(x)),
                                }),
                                transaction: solana_transaction_status::EncodedTransaction::Binary(transaction_info.message.clone(), solana_transaction_status::TransactionBinaryEncoding::Base64)
                            }
                        }).collect_vec())
                    } else {
                        None
                    }
                },
                None => None
            };
            Ok(Some(UiConfirmedBlock {
                previous_blockhash: block.previous_blockhash,
                blockhash: block.blockhash,
                parent_slot: block.parent_slot,
                transactions,
                signatures: None,
                rewards: block.rewards,
                block_time: Some(block.block_time as i64),
                block_height: Some(block.block_height),
            }))
        } else {
            Ok(None)
        }
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
            .into_epoch_info(block_info.block_height, None);
        Ok(epoch_info)
    }
}
