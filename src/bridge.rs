use crate::{
    configs::SendTransactionConfig,
    encoding::BinaryEncoding,
    rpc::LiteRpcServer,
    workers::{BlockListener, TxSender},
};

use std::{ops::Deref, str::FromStr, sync::Arc};

use anyhow::bail;
use log::info;
use reqwest::Url;

use jsonrpsee::{server::ServerBuilder, types::SubscriptionResult, SubscriptionSink};
use solana_client::{
    nonblocking::{rpc_client::RpcClient, tpu_client::TpuClient},
    rpc_config::{RpcContextConfig, RpcRequestAirdropConfig},
    rpc_response::{Response as RpcResponse, RpcBlockhash, RpcResponseContext, RpcVersionInfo},
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    pubkey::Pubkey,
    transaction::VersionedTransaction,
};
use solana_transaction_status::TransactionStatus;
use tokio::{net::ToSocketAddrs, task::JoinHandle};

/// A bridge between clients and tpu
#[derive(Clone)]
pub struct LiteBridge {
    pub tpu_client: Arc<TpuClient>,
    pub rpc_url: Url,
    pub tx_sender: Option<TxSender>,
    pub finalized_block_listenser: BlockListener,
    pub confirmed_block_listenser: BlockListener,
}

impl LiteBridge {
    pub async fn new(
        rpc_url: reqwest::Url,
        ws_addr: &str,
        batch_transactions: bool,
    ) -> anyhow::Result<Self> {
        let rpc_client = Arc::new(RpcClient::new(rpc_url.to_string()));

        let tpu_client =
            Arc::new(TpuClient::new(rpc_client.clone(), ws_addr, Default::default()).await?);

        let finalized_block_listenser =
            BlockListener::new(rpc_client.clone(), ws_addr, CommitmentConfig::finalized()).await?;
        let confirmed_block_listenser =
            BlockListener::new(rpc_client.clone(), ws_addr, CommitmentConfig::confirmed()).await?;

        Ok(Self {
            tx_sender: if batch_transactions {
                Some(TxSender::new(tpu_client.clone()))
            } else {
                None
            },
            finalized_block_listenser,
            confirmed_block_listenser,
            rpc_url,
            tpu_client,
        })
    }

    pub fn get_block_listner(&self, commitment_config: CommitmentConfig) -> BlockListener {
        if let CommitmentLevel::Finalized = commitment_config.commitment {
            self.finalized_block_listenser.clone()
        } else {
            self.confirmed_block_listenser.clone()
        }
    }

    /// List for `JsonRpc` requests
    pub async fn start_services<T: ToSocketAddrs + std::fmt::Debug + 'static + Send + Clone>(
        self,
        http_addr: T,
        ws_addr: T,
    ) -> anyhow::Result<Vec<JoinHandle<anyhow::Result<()>>>> {
        let tx_sender = self.tx_sender.clone();

        let finalized_block_listenser = self.finalized_block_listenser.clone().listen();

        let confirmed_block_listenser = self.confirmed_block_listenser.clone().listen();

        let ws_server_handle = ServerBuilder::default()
            .ws_only()
            .build(ws_addr.clone())
            .await?
            .start(self.clone().into_rpc())?;

        let http_server_handle = ServerBuilder::default()
            .http_only()
            .build(http_addr.clone())
            .await?
            .start(self.into_rpc())?;

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

        let mut services = vec![
            ws_server,
            http_server,
            finalized_block_listenser,
            confirmed_block_listenser,
        ];

        if let Some(tx_sender) = tx_sender {
            services.push(tx_sender.execute());
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
        let SendTransactionConfig {
            encoding,
            max_retries: _,
        } = send_transaction_config.unwrap_or_default();

        let raw_tx = encoding.decode(tx).unwrap();

        let sig = bincode::deserialize::<VersionedTransaction>(&raw_tx)
            .unwrap()
            .signatures[0];

        if let Some(tx_sender) = &self.tx_sender {
            tx_sender.enqnueue_tx(raw_tx);
        } else {
            self.tpu_client.send_wire_transaction(raw_tx.clone()).await;
        }

        Ok(BinaryEncoding::Base58.encode(sig))
    }

    async fn get_latest_blockhash(
        &self,
        config: Option<solana_client::rpc_config::RpcContextConfig>,
    ) -> crate::rpc::Result<RpcResponse<solana_client::rpc_response::RpcBlockhash>> {
        let commitment_config = if let Some(RpcContextConfig { commitment, .. }) = config {
            commitment.unwrap_or_default()
        } else {
            CommitmentConfig::default()
        };

        let block_listner = self.get_block_listner(commitment_config);
        let (blockhash, last_valid_block_height) = block_listner.get_latest_blockhash().await;
        let slot = block_listner.get_slot().await;

        Ok(RpcResponse {
            context: RpcResponseContext {
                slot,
                api_version: None,
            },
            value: RpcBlockhash {
                blockhash,
                last_valid_block_height,
            },
        })
    }

    async fn get_signature_statuses(
        &self,
        sigs: Vec<String>,
        _config: Option<solana_client::rpc_config::RpcSignatureStatusConfig>,
    ) -> crate::rpc::Result<RpcResponse<Vec<Option<TransactionStatus>>>> {
        let mut sig_statuses = self
            .confirmed_block_listenser
            .get_signature_statuses(&sigs)
            .await;

        // merge
        let mut sig_index = 0;
        for finalized_block in self
            .finalized_block_listenser
            .get_signature_statuses(&sigs)
            .await
        {
            if let Some(finalized_block) = finalized_block {
                sig_statuses[sig_index] = Some(finalized_block);
            }
            sig_index += 0;
        }

        Ok(RpcResponse {
            context: RpcResponseContext {
                slot: self.finalized_block_listenser.get_slot().await,
                api_version: None,
            },
            value: sig_statuses,
        })
    }

    fn get_version(&self) -> crate::rpc::Result<RpcVersionInfo> {
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
        let pubkey = Pubkey::from_str(&pubkey_str).unwrap();

        Ok(self
            .tpu_client
            .rpc_client()
            .request_airdrop_with_config(&pubkey, lamports, config.unwrap_or_default())
            .await
            .unwrap()
            .to_string())
    }

    fn signature_subscribe(
        &self,
        mut sink: SubscriptionSink,
        signature: String,
        commitment_config: CommitmentConfig,
    ) -> SubscriptionResult {
        sink.accept()?;
        self.get_block_listner(commitment_config)
            .signature_subscribe(signature, sink);
        Ok(())
    }
}

impl Deref for LiteBridge {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        self.tpu_client.rpc_client()
    }
}
