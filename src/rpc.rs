use jsonrpsee::core::Error;
use jsonrpsee::proc_macros::rpc;
use solana_client::rpc_config::{
    RpcContextConfig, RpcRequestAirdropConfig, RpcSignatureStatusConfig,
};
use solana_client::rpc_response::{Response as RpcResponse, RpcBlockhash, RpcVersionInfo};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::TransactionStatus;

use crate::configs::SendTransactionConfig;

pub type Result<T> = std::result::Result<T, Error>;

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

    #[subscription(name = "signatureSubscribe", unsubscribe="signatureUnsubscribe", item=RpcResponse<Option<TransactionError>>)]
    fn signature_subscribe(&self, signature: String, commitment_config: CommitmentConfig);
}
