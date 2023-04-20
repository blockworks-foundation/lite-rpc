use crate::configs::{IsBlockHashValidConfig, SendTransactionConfig};
use crate::errors::RpcCustomResult;
use jsonrpsee::{core::SubscriptionResult, proc_macros::rpc};
use solana_rpc_client_api::config::{
    RpcContextConfig, RpcRequestAirdropConfig, RpcSignatureStatusConfig,
};
use solana_rpc_client_api::response::{Response as RpcResponse, RpcBlockhash, RpcVersionInfo};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::TransactionStatus;

#[rpc(server)]
pub trait LiteRpc {
    #[method(name = "sendTransaction")]
    async fn send_transaction(
        &self,
        tx: String,
        send_transaction_config: Option<SendTransactionConfig>,
    ) -> RpcCustomResult<String>;

    #[method(name = "getLatestBlockhash")]
    async fn get_latest_blockhash(
        &self,
        config: Option<RpcContextConfig>,
    ) -> RpcCustomResult<RpcResponse<RpcBlockhash>>;

    #[method(name = "isBlockhashValid")]
    async fn is_blockhash_valid(
        &self,
        blockhash: String,
        config: Option<IsBlockHashValidConfig>,
    ) -> RpcCustomResult<RpcResponse<bool>>;

    #[method(name = "getSignatureStatuses")]
    async fn get_signature_statuses(
        &self,
        signature_strs: Vec<String>,
        config: Option<RpcSignatureStatusConfig>,
    ) -> RpcCustomResult<RpcResponse<Vec<Option<TransactionStatus>>>>;

    #[method(name = "getVersion")]
    fn get_version(&self) -> RpcCustomResult<RpcVersionInfo>;

    #[method(name = "requestAirdrop")]
    async fn request_airdrop(
        &self,
        pubkey_str: String,
        lamports: u64,
        config: Option<RpcRequestAirdropConfig>,
    ) -> RpcCustomResult<String>;

    #[subscription(name = "signatureSubscribe" => "signatureNotification", unsubscribe="signatureUnsubscribe", item=RpcResponse<serde_json::Value>)]
    async fn signature_subscribe(
        &self,
        signature: String,
        commitment_config: CommitmentConfig,
    ) -> SubscriptionResult;
}
