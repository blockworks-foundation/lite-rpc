use crate::{
    configs::SendTransactionConfig,
    encoding::BinaryEncoding,
    rpc::LiteRpcServer,
    workers::{BlockListener, TxSender},
};

use std::{ops::Deref, sync::Arc};

use anyhow::bail;
use reqwest::Url;

use jsonrpsee::server::ServerBuilder;
use solana_client::{
    nonblocking::{rpc_client::RpcClient, tpu_client::TpuClient},
    rpc_config::RpcContextConfig,
    rpc_response::{Response as RpcResponse, RpcBlockhash, RpcResponseContext, RpcVersionInfo},
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
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
    pub async fn start_services(
        self,
        addr: impl ToSocketAddrs,
    ) -> anyhow::Result<Vec<JoinHandle<anyhow::Result<()>>>> {
        let tx_sender = self.tx_sender.clone();

        let finalized_block_listenser = self.finalized_block_listenser.clone().listen();

        let confirmed_block_listenser = self.confirmed_block_listenser.clone().listen();

        let handle = ServerBuilder::default()
            .build(addr)
            .await?
            .start(self.into_rpc())?;

        let server = tokio::spawn(async move {
            handle.stopped().await;
            bail!("server stopped");
        });

        let mut services = vec![server, finalized_block_listenser, confirmed_block_listenser];

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
        SendTransactionConfig {
            encoding,
            max_retries: _,
        }: SendTransactionConfig,
    ) -> crate::rpc::Result<String> {
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
        let slot = block_listner.get_slot();

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
                slot: self.finalized_block_listenser.get_slot(),
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
}

impl Deref for LiteBridge {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        self.tpu_client.rpc_client()
    }
}
