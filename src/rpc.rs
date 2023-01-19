use jsonrpsee::proc_macros::rpc;
use solana_client::rpc_config::{
    RpcContextConfig, RpcRequestAirdropConfig, RpcSignatureStatusConfig,
};
use solana_client::rpc_response::{Response as RpcResponse, RpcBlockhash, RpcVersionInfo};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::TransactionStatus;

use crate::{
    configs::{IsBlockHashValidConfig, SendTransactionConfig},
    workers::Metrics,
};

pub type Result<T> = std::result::Result<T, jsonrpsee::core::Error>;

#[rpc(server)]
pub trait LiteRpc {
    #[method(name = "sendTransaction")]
    async fn send_transaction(
        &self,
        tx: String,
        send_transaction_config: Option<SendTransactionConfig>,
    ) -> Result<String>;

    #[method(name = "getLatestBlockhash")]
    async fn get_latest_blockhash(
        &self,
        config: Option<RpcContextConfig>,
    ) -> Result<RpcResponse<RpcBlockhash>>;

    #[method(name = "isBlockhashValid")]
    async fn is_blockhash_valid(
        &self,
        blockhash: String,
        config: Option<IsBlockHashValidConfig>,
    ) -> Result<RpcResponse<bool>>;

    #[method(name = "getSignatureStatuses")]
    async fn get_signature_statuses(
        &self,
        signature_strs: Vec<String>,
        config: Option<RpcSignatureStatusConfig>,
    ) -> Result<RpcResponse<Vec<Option<TransactionStatus>>>>;

    #[method(name = "getVersion")]
    fn get_version(&self) -> Result<RpcVersionInfo>;

    #[method(name = "requestAirdrop")]
    async fn request_airdrop(
        &self,
        pubkey_str: String,
        lamports: u64,
        config: Option<RpcRequestAirdropConfig>,
    ) -> Result<String>;

    #[method(name = "getMetrics")]
    async fn get_metrics(&self) -> Result<Metrics>;

    #[subscription(name = "signatureSubscribe" => "signatureNotification", unsubscribe="signatureUnsubscribe", item=RpcResponse<serde_json::Value>)]
    fn signature_subscribe(&self, signature: String, commitment_config: CommitmentConfig);
}
