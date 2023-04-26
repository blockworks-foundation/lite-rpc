use crate::{
    block_store::{BlockInformation, BlockStore},
    configs::{IsBlockHashValidConfig, SendTransactionConfig},
    encoding::BinaryEncoding,
    rpc::LiteRpcServer,
    workers::{
        tpu_utils::tpu_service::TpuService, BlockListener, Cleaner, MetricsCapture, Postgres,
        PrometheusSync, TransactionReplay, TransactionReplayer, TxSender, WireTransaction,
        MESSAGES_IN_REPLAY_QUEUE,
    },
    DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE,
};

use std::{ops::Deref, str::FromStr, sync::Arc, time::Duration};

use anyhow::bail;

use log::{error, info};

use jsonrpsee::{core::SubscriptionResult, server::ServerBuilder, PendingSubscriptionSink};

use prometheus::{core::GenericGauge, opts, register_int_counter, register_int_gauge, IntCounter};
use solana_rpc_client::{nonblocking::rpc_client::RpcClient, rpc_client::SerializableTransaction};
use solana_rpc_client_api::{
    config::{RpcContextConfig, RpcRequestAirdropConfig, RpcSignatureStatusConfig},
    response::{Response as RpcResponse, RpcBlockhash, RpcResponseContext, RpcVersionInfo},
};
use solana_sdk::{
    commitment_config::CommitmentConfig, hash::Hash, pubkey::Pubkey, signature::Keypair,
    transaction::VersionedTransaction,
};
use solana_transaction_status::TransactionStatus;
use tokio::{
    net::ToSocketAddrs,
    sync::mpsc::{self, Sender, UnboundedSender},
    task::JoinHandle,
    time::Instant,
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
    pub static ref TXS_IN_CHANNEL: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_txs_in_channel", "Transactions in channel")).unwrap();
}

/// A bridge between clients and tpu
pub struct LiteBridge {
    pub rpc_client: Arc<RpcClient>,
    pub tpu_service: Arc<TpuService>,
    // None if LiteBridge is not executed
    pub tx_send_channel: Option<Sender<(String, WireTransaction, u64)>>,
    pub tx_sender: TxSender,
    pub block_listner: BlockListener,
    pub block_store: BlockStore,

    pub tx_replayer: TransactionReplayer,
    pub tx_replay_sender: Option<UnboundedSender<TransactionReplay>>,
    pub max_retries: usize,
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

        let tpu_service = TpuService::new(
            current_slot,
            fanout_slots,
            Arc::new(identity),
            rpc_client.clone(),
            ws_addr,
        )
        .await?;
        let tpu_service = Arc::new(tpu_service);

        let tx_sender = TxSender::new(tpu_service.clone());

        let block_store = BlockStore::new(&rpc_client).await?;

        let block_listner =
            BlockListener::new(rpc_client.clone(), tx_sender.clone(), block_store.clone());

        let tx_replayer = TransactionReplayer::new(tx_sender.clone(), retry_after);
        Ok(Self {
            rpc_client,
            tpu_service,
            tx_send_channel: None,
            tx_sender,
            block_listner,
            block_store,
            tx_replayer,
            tx_replay_sender: None,
            max_retries,
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
    ) -> anyhow::Result<Vec<JoinHandle<anyhow::Result<()>>>> {
        let (postgres, postgres_send) = if enable_postgres {
            let (postgres_send, postgres_recv) = mpsc::unbounded_channel();
            let postgres = Postgres::new().await?;
            let postgres = postgres.start(postgres_recv);

            (Some(postgres), Some(postgres_send))
        } else {
            (None, None)
        };

        let mut tpu_services = self.tpu_service.start().await?;

        let (tx_send, tx_recv) = mpsc::channel(DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE);
        self.tx_send_channel = Some(tx_send);

        let tx_sender = self
            .tx_sender
            .clone()
            .execute(tx_recv, postgres_send.clone());

        let (replay_sender, replay_reciever) = tokio::sync::mpsc::unbounded_channel();
        let replay_service = self
            .tx_replayer
            .start_service(replay_sender.clone(), replay_reciever);
        self.tx_replay_sender = Some(replay_sender);

        let metrics_capture = MetricsCapture::new(self.tx_sender.clone()).capture();
        let prometheus_sync = PrometheusSync.sync(prometheus_addr);

        let finalized_block_listener = self
            .block_listner
            .clone()
            .listen(CommitmentConfig::finalized(), postgres_send.clone());

        let confirmed_block_listener = self
            .block_listner
            .clone()
            .listen(CommitmentConfig::confirmed(), None);

        let processed_block_listener = self.block_listner.clone().listen_processed();

        let cleaner = Cleaner::new(
            self.tx_sender.clone(),
            self.block_listner.clone(),
            self.block_store.clone(),
        )
        .start(clean_interval);

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

            let ws_server = tokio::spawn(async move {
                info!("Websocket Server started at {ws_addr:?}");
                ws_server_handle.stopped().await;
                bail!("Websocket server stopped");
            });

            let http_server = tokio::spawn(async move {
                info!("HTTP Server started at {http_addr:?}");
                http_server_handle.stopped().await;
                bail!("HTTP server stopped");
            });

            (ws_server, http_server)
        };

        let mut services = vec![
            ws_server,
            http_server,
            tx_sender,
            finalized_block_listener,
            confirmed_block_listener,
            processed_block_listener,
            metrics_capture,
            prometheus_sync,
            cleaner,
            replay_service,
        ];

        services.append(&mut tpu_services);

        if let Some(postgres) = postgres {
            services.push(postgres);
        }

        Ok(services)
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

        let tx = match bincode::deserialize::<VersionedTransaction>(&raw_tx) {
            Ok(tx) => tx,
            Err(err) => {
                return Err(jsonrpsee::core::Error::Custom(err.to_string()));
            }
        };

        let sig = tx.get_signature();
        let Some(BlockInformation { slot, .. }) = self
            .block_store
            .get_block_info(&tx.get_recent_blockhash().to_string())
        else {
            log::warn!("block");
            return Err(jsonrpsee::core::Error::Custom("Blockhash not found in block store".to_string()));
        };

        let raw_tx_clone = raw_tx.clone();
        if let Err(e) = self
            .tx_send_channel
            .as_ref()
            .expect("Lite Bridge Not Executed")
            .send((sig.to_string(), raw_tx, slot))
            .await
        {
            error!(
                "Internal error sending transaction on send channel error {}",
                e
            );
        }

        if let Some(tx_replay_sender) = &self.tx_replay_sender {
            let max_replay = max_retries.map_or(self.max_retries, |x| x as usize);
            let replay_at = Instant::now() + self.tx_replayer.retry_after;
            // ignore error for replay service
            if tx_replay_sender
                .send(TransactionReplay {
                    signature: sig.to_string(),
                    tx: raw_tx_clone,
                    replay_count: 0,
                    max_replay,
                    replay_at,
                })
                .is_ok()
            {
                MESSAGES_IN_REPLAY_QUEUE.inc();
            }
        }
        TXS_IN_CHANNEL.inc();

        Ok(BinaryEncoding::Base58.encode(sig))
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
            .map(|sig| {
                self.tx_sender
                    .txs_sent_store
                    .get(sig)
                    .and_then(|v| v.status.clone())
            })
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

        self.tx_sender
            .txs_sent_store
            .insert(airdrop_sig.clone(), Default::default());

        Ok(airdrop_sig)
    }

    async fn signature_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        signature: String,
        commitment_config: CommitmentConfig,
    ) -> SubscriptionResult {
        RPC_SIGNATURE_SUBSCRIBE.inc();
        let sink = pending.accept().await?;

        self.block_listner
            .signature_subscribe(signature, commitment_config, sink);

        Ok(())
    }
}

impl Deref for LiteBridge {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.rpc_client
    }
}
