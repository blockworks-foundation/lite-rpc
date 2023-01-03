use std::net::SocketAddr;

use jsonrpc_core::{MetaIoHandler, Result};
use jsonrpc_derive::rpc;
use jsonrpc_http_server::ServerBuilder;
use solana_client::rpc_config::{
    RpcContextConfig, RpcSendTransactionConfig, RpcSignatureStatusConfig,
};
use solana_client::rpc_response::{Response as RpcResponse, RpcBlockhash, RpcVersionInfo};
use solana_transaction_status::TransactionStatus;

use crate::bridge::LiteBridge;
use async_trait::async_trait;
use tokio::task::JoinHandle;

#[rpc]
pub trait Lite {
    type Metadata;

    #[rpc(meta, name = "sendTransaction")]
    fn send_transaction(
        &self,
        meta: Self::Metadata,
        data: String,
        config: Option<RpcSendTransactionConfig>,
    ) -> Result<String>;

    #[rpc(meta, name = "getLatestBlockhash")]
    fn get_latest_blockhash(
        &self,
        meta: Self::Metadata,
        config: Option<RpcContextConfig>,
    ) -> Result<RpcResponse<RpcBlockhash>>;

    #[rpc(meta, name = "getSignatureStatuses")]
    fn get_signature_statuses(
        &self,
        meta: Self::Metadata,
        signature_strs: Vec<String>,
        config: Option<RpcSignatureStatusConfig>,
    ) -> Result<RpcResponse<Vec<Option<TransactionStatus>>>>;

    #[rpc(name = "getVersion")]
    fn get_version(&self) -> Result<RpcVersionInfo>;
}

struct LiteRpc;

#[async_trait]
impl Lite for LiteRpc {
    type Metadata = LiteBridge;

    async fn send_transaction(
        &self,
        meta: Self::Metadata,
        tx: String,
        send_transaction_config: Option<RpcSendTransactionConfig>,
    ) -> Result<String> {
        Ok(meta.send_transaction(tx, send_transaction_config).await?)
    }

    fn get_latest_blockhash(
        &self,
        meta: Self::Metadata,
        config: Option<RpcContextConfig>,
    ) -> Result<RpcResponse<RpcBlockhash>> {
        todo!()
    }

    fn get_signature_statuses(
        &self,
        meta: Self::Metadata,
        signature_strs: Vec<String>,
        config: Option<RpcSignatureStatusConfig>,
    ) -> Result<RpcResponse<Vec<Option<TransactionStatus>>>> {
        todo!()
    }

    fn get_version(&self) -> Result<RpcVersionInfo> {
        todo!()
    }
}

impl LiteRpc {
    fn start_server(bridge: LiteBridge, addr: SocketAddr) -> JoinHandle<anyhow::Result<()>> {
        tokio::spawn(async move {
            ServerBuilder::with_meta_extractor(MetaIoHandler::default(), move |_| bridge.clone())
                .start_http(&addr)?;

            Ok(())
        })
    }
}
