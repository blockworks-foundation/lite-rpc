use crate::{
    configs::{IsBlockHashValidConfig, SendTransactionConfig},
    jsonrpsee_subscrption_handler_sink::JsonRpseeSubscriptionHandlerSink,
    postgres::Postgres,
    rpc::LiteRpcServer,
    DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE,
};

use solana_lite_rpc_services::{
    block_listenser::BlockListener,
    metrics_capture::MetricsCapture,
    prometheus_sync::PrometheusSync,
    tpu_utils::tpu_service::TpuService,
    transaction_replayer::TransactionReplayer,
    transaction_service::{TransactionService, TransactionServiceBuilder},
    tx_sender::WireTransaction,
    tx_sender::{TxSender, TXS_IN_CHANNEL},
};

use anyhow::bail;
use jsonrpsee::{core::SubscriptionResult, server::ServerBuilder, PendingSubscriptionSink};
use log::info;
use prometheus::{opts, register_int_counter, IntCounter};
use solana_lite_rpc_core::{
    block_store::{BlockInformation, BlockStore},
    tx_store::{empty_tx_store, TxStore},
    AnyhowJoinHandle,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::{
    config::{RpcContextConfig, RpcRequestAirdropConfig, RpcSignatureStatusConfig},
    response::{Response as RpcResponse, RpcBlockhash, RpcResponseContext, RpcVersionInfo},
};
use solana_sdk::{
    commitment_config::CommitmentConfig, hash::Hash, pubkey::Pubkey, signature::Keypair,
    slot_history::Slot,
};
use solana_transaction_status::TransactionStatus;
use std::{ops::Deref, str::FromStr, sync::Arc, time::Duration};
use tokio::{
    net::ToSocketAddrs,
    sync::mpsc::{self, Sender},
};

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
    pub rpc_client: Arc<RpcClient>,
    pub tx_store: TxStore,
    // None if LiteBridge is not executed
    pub tx_send_channel: Option<Sender<(String, WireTransaction, u64)>>,
    pub block_store: BlockStore,
    pub max_retries: usize,
    pub transaction_service_builder: TransactionServiceBuilder,
    pub transaction_service: Option<TransactionService>,
    pub block_listner: BlockListener,
}

impl LiteBridge {
    pub async fn new(
        rpc_url: String,
        ws_addr: String,
        fanout_slots: u64,
        identity: Keypair,
        retry_after: Duration,
        max_retries: usize,
    ) -> anyhow::Result<Self> {
        let rpc_client = Arc::new(RpcClient::new(rpc_url.clone()));
        let current_slot = rpc_client.get_slot().await?;

        let tx_store = empty_tx_store();

        let tpu_service = TpuService::new(
            current_slot,
            fanout_slots,
            Arc::new(identity),
            rpc_client.clone(),
            ws_addr,
            tx_store.clone(),
        )
        .await?;

        let tx_sender = TxSender::new(tx_store.clone(), tpu_service.clone());

        let block_store = BlockStore::new(&rpc_client).await?;

        let block_listner =
            BlockListener::new(rpc_client.clone(), tx_store.clone(), block_store.clone());

        let tx_replayer =
            TransactionReplayer::new(tpu_service.clone(), tx_store.clone(), retry_after);

        let transaction_manager = TransactionServiceBuilder::new(
            tx_sender,
            tx_replayer,
            block_listner.clone(),
            tpu_service,
            DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE,
        );

        Ok(Self {
            rpc_client,
            tx_store,
            tx_send_channel: None,
            block_store,
            max_retries,
            transaction_service_builder: transaction_manager,
            transaction_service: None,
            block_listner,
        })
    }

    /// List for `JsonRpc` requests
    #[allow(clippy::too_many_arguments)]
    pub async fn start_services<T: ToSocketAddrs + std::fmt::Debug + 'static + Send + Clone>(
        mut self,
        http_addr: T,
        ws_addr: T,
        clean_interval: Duration,
        enable_postgres: bool,
        prometheus_addr: T,
    ) -> anyhow::Result<()> {
        let (postgres, postgres_send) = if enable_postgres {
            let (postgres_send, postgres_recv) = mpsc::unbounded_channel();
            let postgres = Postgres::new().await?;
            let postgres = postgres.start(postgres_recv);

            (Some(postgres), Some(postgres_send))
        } else {
            (None, None)
        };

        let metrics_capture = MetricsCapture::new(self.tx_store.clone()).capture();
        let prometheus_sync = PrometheusSync.sync(prometheus_addr);

        let max_retries = self.max_retries;
        let (transaction_service, jh_transaction_services) = self
            .transaction_service_builder
            .start(
                postgres_send,
                self.block_store.clone(),
                max_retries,
                clean_interval,
            )
            .await;
        self.transaction_service = Some(transaction_service);
        let rpc = self.into_rpc();

        let (ws_server, http_server) = {
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
                info!("Websocket Server started at {ws_addr:?}");
                ws_server_handle.stopped().await;
                bail!("Websocket server stopped");
            });

            let http_server: AnyhowJoinHandle = tokio::spawn(async move {
                info!("HTTP Server started at {http_addr:?}");
                http_server_handle.stopped().await;
                bail!("HTTP server stopped");
            });

            (ws_server, http_server)
        };

        let postgres = tokio::spawn(async {
            let Some(postgres) = postgres else {
                std::future::pending::<()>().await;
                unreachable!();
            };

            postgres.await
        });

        tokio::select! {
            res = ws_server => {
                bail!("WebSocket server exited unexpectedly {res:?}");
            },
            res = http_server => {
                bail!("HTTP server exited unexpectedly {res:?}");
            },
            res = metrics_capture => {
                bail!("Metrics Capture exited unexpectedly {res:?}");
            },
            res = prometheus_sync => {
                bail!("Prometheus Service exited unexpectedly {res:?}");
            },
            res = postgres => {
                bail!("Postgres service exited unexpectedly {res:?}");
            },
            res = jh_transaction_services => {
                bail!("Transaction service exited unexpectedly {res:?}");
            }
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

        let transaction_service = self
            .transaction_service
            .clone()
            .expect("Transaction Service should have been initialized");

        match transaction_service
            .send_transaction(raw_tx, max_retries)
            .await
        {
            Ok(sig) => {
                println!("sig: {}", sig);
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

        let (
            blockhash,
            BlockInformation {
                slot, block_height, ..
            },
        ) = self.block_store.get_latest_block(commitment_config).await;

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

        let blockhash = match Hash::from_str(&blockhash) {
            Ok(blockhash) => blockhash,
            Err(err) => {
                return Err(jsonrpsee::core::Error::Custom(err.to_string()));
            }
        };

        let is_valid = match self
            .rpc_client
            .is_blockhash_valid(&blockhash, commitment)
            .await
        {
            Ok(is_valid) => is_valid,
            Err(err) => {
                return Err(jsonrpsee::core::Error::Custom(err.to_string()));
            }
        };

        let slot = self
            .block_store
            .get_latest_block_info(commitment)
            .await
            .slot;

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
            .map(|sig| self.tx_store.get(sig).and_then(|v| v.status.clone()))
            .collect();

        Ok(RpcResponse {
            context: RpcResponseContext {
                slot: self
                    .block_store
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
        {
            Ok(airdrop_sig) => airdrop_sig.to_string(),
            Err(err) => {
                return Err(jsonrpsee::core::Error::Custom(err.to_string()));
            }
        };

        self.tx_store
            .insert(airdrop_sig.clone(), Default::default());

        Ok(airdrop_sig)
    }

    async fn get_slot(&self, config: Option<RpcContextConfig>) -> crate::rpc::Result<Slot> {
        let commitment_config = config
            .map(|config| config.commitment.unwrap_or_default())
            .unwrap_or_default();

        let (_, BlockInformation { slot, .. }) =
            self.block_store.get_latest_block(commitment_config).await;
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
        self.block_listner.signature_subscribe(
            signature,
            commitment_config,
            Arc::new(jsonrpsee_sink),
        );

        Ok(())
    }
}

impl Deref for LiteBridge {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.rpc_client
    }
}
