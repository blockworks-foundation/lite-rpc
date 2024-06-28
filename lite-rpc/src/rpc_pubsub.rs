use jsonrpsee::core::SubscriptionResult;
use jsonrpsee::proc_macros::rpc;
use solana_rpc_client_api::config::{
    RpcAccountInfoConfig, RpcBlockSubscribeConfig, RpcBlockSubscribeFilter,
    RpcProgramAccountsConfig, RpcSignatureSubscribeConfig, RpcTransactionLogsConfig,
    RpcTransactionLogsFilter,
};

pub type Result<T> = std::result::Result<T, jsonrpsee::core::Error>;

#[rpc(server)]
pub trait LiteRpcPubSub {
    // ***********************
    // Direct Subscription Domain
    // ***********************

    #[subscription(name = "slotSubscribe" => "slotNotification", unsubscribe="slotUnsubscribe", item=String)]
    async fn slot_subscribe(&self) -> SubscriptionResult;

    #[subscription(name = "blockSubscribe" => "blockNotification", unsubscribe="blockUnsubscribe", item=String)]
    async fn block_subscribe(
        &self,
        filter: RpcBlockSubscribeFilter,
        config: Option<RpcBlockSubscribeConfig>,
    ) -> SubscriptionResult;

    // ***********************
    // Indirect Subscription Domain
    // ***********************

    #[subscription(name = "logsSubscribe" => "logsNotification", unsubscribe="logsUnsubscribe", item=String)]
    async fn logs_subscribe(
        &self,
        filter: RpcTransactionLogsFilter,
        config: Option<RpcTransactionLogsConfig>,
    ) -> SubscriptionResult;

    // WARN: enable_received_notification: bool is ignored
    #[subscription(name = "signatureSubscribe" => "signatureNotification", unsubscribe="signatureUnsubscribe", item=String)]
    async fn signature_subscribe(
        &self,
        signature: String,
        config: RpcSignatureSubscribeConfig,
    ) -> SubscriptionResult;

    #[subscription(name = "slotUpdatesSubscribe" => "slotUpdatesNotification", unsubscribe="slotUpdatesUnsubscribe", item=String)]
    async fn slot_updates_subscribe(&self) -> SubscriptionResult;

    #[subscription(name = "voteSubscribe" => "voteNotification", unsubscribe="voteUnsubscribe", item=String)]
    async fn vote_subscribe(&self) -> SubscriptionResult;

    /// subscribe to prio fees distribution per block; uses confirmation level "confirmed"
    #[subscription(name = "blockPrioritizationFeesSubscribe" => "blockPrioritizationFeesNotification", unsubscribe="blockPrioritizationFeesUnsubscribe", item=String)]
    async fn latest_block_priofees_subscribe(&self) -> SubscriptionResult;

    #[subscription(name = "accountPrioritizationFeesSubscribe" => "accountPrioritizationFeesNotification", unsubscribe="accountPrioritizationFeesUnsubscribe", item=String)]
    async fn latest_account_priofees_subscribe(&self, account: String) -> SubscriptionResult;

    #[subscription(name = "accountSubscribe" => "accountNotification", unsubscribe="accountUnsubscribe", item=String)]
    async fn account_subscribe(
        &self,
        account: String,
        config: Option<RpcAccountInfoConfig>,
    ) -> SubscriptionResult;

    #[subscription(name = "programSubscribe" => "programNotification", unsubscribe="programUnsubscribe", item=String)]
    async fn program_subscribe(
        &self,
        pubkey_str: String,
        config: Option<RpcProgramAccountsConfig>,
    ) -> SubscriptionResult;
}
