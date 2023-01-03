use crate::{
    configs::SendTransactionConfig,
    encoding::BinaryEncoding,
    errors::JsonRpcError,
    workers::{BlockListener, TxSender},
};

use std::{ops::Deref, sync::Arc};

use reqwest::Url;

use solana_client::{
    nonblocking::{rpc_client::RpcClient, tpu_client::TpuClient},
    rpc_response::{Response as RpcResponse, RpcResponseContext, RpcVersionInfo},
};
use solana_sdk::{commitment_config::CommitmentConfig, transaction::VersionedTransaction};
use solana_transaction_status::TransactionStatus;
use tokio::task::JoinHandle;

/// A bridge between clients and tpu
#[derive(Clone)]
pub struct LiteBridge {
    pub tpu_client: Arc<TpuClient>,
    pub rpc_url: Url,
    pub tx_sender: Option<TxSender>,
    pub block_listner: BlockListener,
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

        let block_listner = BlockListener::new(rpc_client.clone(), ws_addr).await?;

        Ok(Self {
            tx_sender: if batch_transactions {
                Some(TxSender::new(tpu_client.clone()))
            } else {
                None
            },
            block_listner,
            rpc_url,
            tpu_client,
        })
    }

    pub async fn send_transaction(
        &self,
        tx: String,
        SendTransactionConfig {
            encoding,
            max_retries: _,
        }: SendTransactionConfig,
    ) -> Result<String, JsonRpcError> {
        let raw_tx = encoding.decode(tx)?;

        let sig = bincode::deserialize::<VersionedTransaction>(&raw_tx)?.signatures[0];

        if let Some(tx_sender) = &self.tx_sender {
            tx_sender.enqnueue_tx(raw_tx);
        } else {
            self.tpu_client.send_wire_transaction(raw_tx.clone()).await;
        }

        Ok(BinaryEncoding::Base58.encode(sig))
    }

    pub async fn get_signature_statuses(
        &self,
        sigs: Vec<String>,
    ) -> Result<RpcResponse<Vec<Option<TransactionStatus>>>, JsonRpcError> {
        Ok(RpcResponse {
            context: RpcResponseContext {
                slot: self.block_listner.get_slot(),
                api_version: None,
            },
            value: self.block_listner.get_signature_statuses(&sigs).await,
        })
    }

    pub fn get_version(&self) -> RpcVersionInfo {
        let version = solana_version::Version::default();
        RpcVersionInfo {
            solana_core: version.to_string(),
            feature_set: Some(version.feature_set),
        }
    }

    /// List for `JsonRpc` requests
    pub fn start_services(self) -> Vec<JoinHandle<anyhow::Result<()>>> {
        let this = Arc::new(self);
        let tx_sender = this.tx_sender.clone();

        let finalized_block_listenser = this
            .block_listner
            .clone()
            .listen(CommitmentConfig::finalized());

        let confirmed_block_listenser = this
            .block_listner
            .clone()
            .listen(CommitmentConfig::confirmed());

        let mut services = vec![finalized_block_listenser, confirmed_block_listenser];

        if let Some(tx_sender) = tx_sender {
            services.push(tx_sender.execute());
        }

        services
    }
}

impl jsonrpc_core::Metadata for LiteBridge {}

impl Deref for LiteBridge {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        self.tpu_client.rpc_client()
    }
}
