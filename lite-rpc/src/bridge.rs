use crate::{
    configs::{IsBlockHashValidConfig, SendTransactionConfig},
    jsonrpsee_subscrption_handler_sink::JsonRpseeSubscriptionHandlerSink,
    postgres::Postgres,
    rpc::LiteRpcServer,
    DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE,
};

use solana_lite_rpc_core::{
    block_store::{Block, BlockMeta},
    ledger::Ledger,
};
use solana_lite_rpc_services::{
    block_listenser::BlockListener,
    metrics_capture::MetricsCapture,
    prometheus_sync::PrometheusSync,
    tpu_utils::tpu_service::{TpuService, TpuServiceConfig},
    tx_service::{tx_batch_fwd::TxBatchFwd, tx_sender::TxSender},
};

use anyhow::{bail, Context};
use jsonrpsee::{core::SubscriptionResult, server::ServerBuilder, PendingSubscriptionSink};
use prometheus::{opts, register_int_counter, IntCounter};
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
    pub ledger: Ledger,
    pub tx_max_retries: usize,
    pub tx_sender: TxSender,
}

impl LiteBridge {
    pub async fn new(
        fanout_slots: u64,
        identity: Keypair,
        retry_after: Duration,
        max_retries: usize,
    ) -> anyhow::Result<Self> {
        let transaction_manager = TransactionServiceBuilder::new(
            tx_sender,
            tx_replayer,
            block_listner.clone(),
            tpu_service,
            DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE,
        );

        Ok(Self {
            ledger,
            tx_max_retries: max_retries,
            tx_sender,
        })
    }

    /// List for `JsonRpc` requests
    pub async fn start_services<T: ToSocketAddrs + std::fmt::Debug + 'static + Send + Clone>(
        mut self,
        http_addr: T,
        ws_addr: T,
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
        let prometheus_sync = PrometheusSync::sync(prometheus_addr);

        // transaction services
        let (transaction_service, jh_transaction_services) = self
            .transaction_service_builder
            .clone()
            .start(postgres_send, self.block_store.clone(), self.tx_max_retries);

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
                log::info!("Websocket Server started at {ws_addr:?}");
                ws_server_handle.stopped().await;
                anyhow::bail!("Websocket server stopped");
            });

            let http_server: AnyhowJoinHandle = tokio::spawn(async move {
                log::info!("HTTP Server started at {http_addr:?}");
                http_server_handle.stopped().await;
                anyhow::bail!("HTTP server stopped");
            });

            (ws_server, http_server)
        };

        let postgres = tokio::spawn(async {
            let Some(postgres) = postgres else {
                std::future::pending::<()>().await;
                unreachable!();
            };

            let res = postgres.await;
            error!("Postgres server stopped");
            res
        });

        tokio::select! {
            res = ws_server => {
                bail!("WebSocket server {res:?}");
            },
            res = http_server => {
                bail!("HTTP server {res:?}");
            },
            res = metrics_capture => {
                bail!("Metrics Capture {res:?}");
            },
            res = prometheus_sync => {
                bail!("Prometheus Service {res:?}");
            },
            res = postgres => {
                bail!("Postgres service {res:?}");
            },
            res = jh_transaction_services => {
                bail!("Transaction service {res:?}");
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

        let Block {
            blockhash,
            meta: BlockMeta { block_height, .. },
        } = self
            .ledger
            .block_store
            .get_latest_block(&commitment_config)
            .await;

        log::info!("glb {blockhash} {slot} {block_height}");

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
            .context("failed to get blockhash validity")
        {
            Ok(is_valid) => is_valid,
            Err(err) => {
                return Err(jsonrpsee::core::Error::Custom(err.to_string()));
            }
        };

        let slot = self.ledger.clock.get_current_slot();

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
                slot: self.ledger.clock.get_current_slot(),
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
            self.ledger.txs.insert(
                airdrop_sig.clone(),
                solana_lite_rpc_core::tx_store::TxMeta {
                    status: None,
                    last_valid_blockheight: block_height,
                },
            );
        }
        Ok(airdrop_sig)
    }

    async fn get_slot(&self, config: Option<RpcContextConfig>) -> crate::rpc::Result<Slot> {
        let commitment_config = config
            .map(|config| config.commitment.unwrap_or_default())
            .unwrap_or_default();

        let (_, BlockMeta) = self.block_store.get_latest_block(commitment_config).await;
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

        self.ledger
            .tx_subs
            .subscribe((signature, commitment_config), Box::new(jsonrpsee_sink));

        Ok(())
    }
}
