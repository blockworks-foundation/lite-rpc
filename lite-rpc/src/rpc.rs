use crate::configs::{IsBlockHashValidConfig, SendTransactionConfig};
use jsonrpsee::core::SubscriptionResult;
use jsonrpsee::proc_macros::rpc;
use solana_rpc_client_api::config::{
    RpcBlockConfig, RpcBlockSubscribeConfig, RpcBlockSubscribeFilter, RpcBlocksConfigWrapper,
    RpcContextConfig, RpcEncodingConfigWrapper, RpcGetVoteAccountsConfig, RpcLeaderScheduleConfig,
    RpcProgramAccountsConfig, RpcRequestAirdropConfig, RpcSignatureStatusConfig,
    RpcSignatureSubscribeConfig, RpcSignaturesForAddressConfig, RpcTransactionLogsConfig,
    RpcTransactionLogsFilter,
};
use solana_rpc_client_api::response::{
    Response as RpcResponse, RpcBlockhash, RpcConfirmedTransactionStatusWithSignature,
    RpcContactInfo, RpcPerfSample, RpcPrioritizationFee, RpcVersionInfo, RpcVoteAccountStatus,
};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::slot_history::Slot;
use solana_transaction_status::{TransactionStatus, UiConfirmedBlock};
use std::collections::HashMap;
use solana_lite_rpc_block_priofees::block_priofees::PrioFeesStats;

pub type Result<T> = std::result::Result<T, jsonrpsee::core::Error>;

#[rpc(server)]
pub trait LiteRpc {
    // ***********************
    // History Domain
    // ***********************

    #[method(name = "getBlock")]
    async fn get_block(
        &self,
        slot: u64,
        config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
    ) -> Result<Option<UiConfirmedBlock>>;

    #[method(name = "getBlocks")]
    async fn get_blocks(
        &self,
        start_slot: Slot,
        config: Option<RpcBlocksConfigWrapper>,
        commitment: Option<CommitmentConfig>,
    ) -> Result<Vec<Slot>>;

    #[method(name = "getSignaturesForAddress")]
    async fn get_signatures_for_address(
        &self,
        address: String,
        config: Option<RpcSignaturesForAddressConfig>,
    ) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>>;

    // issue:  solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta does not implement Clone
    //
    //#[method(name = "getTransaction")]
    //async fn get_transaction(
    //    &self,
    //    signature_str: String,
    //    config: Option<RpcEncodingConfigWrapper<RpcTransactionConfig>>,
    //) -> Result<Option<EncodedConfirmedTransactionWithStatusMeta>>;

    // ***********************
    // Cluster Domain
    // ***********************

    #[method(name = "getClusterNodes")]
    async fn get_cluster_nodes(&self) -> Result<Vec<RpcContactInfo>>;

    // ***********************
    // Validator Domain
    // ***********************

    #[method(name = "getSlot")]
    async fn get_slot(&self, config: Option<RpcContextConfig>) -> Result<Slot>;

    #[method(name = "getBlockHeight")]
    async fn get_block_height(&self, config: Option<RpcContextConfig>) -> Result<u64>;

    #[method(name = "getBlockTime")]
    async fn get_block_time(&self, block: u64) -> Result<u64>;

    #[method(name = "getFirstAvailableBlock")]
    async fn get_first_available_block(&self) -> Result<u64>;

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

    // BlockCommitmentArray is defined in solana/runtime/src/commitment.rs
    //
    // pub type BlockCommitmentArray = [u64; MAX_LOCKOUT_HISTORY + 1];
    //
    // where
    // solana_vote_program::vote_state::MAX_LOCKOUT_HISTORY,
    //
    // Maximum number of votes to keep around, tightly coupled with epoch_schedule::MINIMUM_SLOTS_PER_EPOCH
    // pub const MAX_LOCKOUT_HISTORY: usize = 31;
    //
    // #[method(name = "getBlockCommitment")]
    // async fn get_block_commitment(
    //     &self,
    //     block: u64,
    // ) -> Result<RpcBlockCommitment<BlockCommitmentArray>>;

    #[method(name = "getRecentPerformanceSamples")]
    async fn get_recent_performance_samples(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<RpcPerfSample>>;

    #[method(name = "getSignatureStatuses")]
    async fn get_signature_statuses(
        &self,
        signature_strs: Vec<String>,
        config: Option<RpcSignatureStatusConfig>,
    ) -> Result<RpcResponse<Vec<Option<TransactionStatus>>>>;

    #[method(name = "getRecentPrioritizationFees")]
    async fn get_recent_prioritization_fees(
        &self,
        pubkey_strs: Option<Vec<String>>,
    ) -> Result<Vec<RpcPrioritizationFee>>;

    // ***********************
    // Send Transaction Domain
    // ***********************

    #[method(name = "sendTransaction")]
    async fn send_transaction(
        &self,
        tx: String,
        send_transaction_config: Option<SendTransactionConfig>,
    ) -> Result<String>;

    // ***********************
    // Deprecated
    // ***********************

    #[method(name = "getVersion")]
    fn get_version(&self) -> Result<RpcVersionInfo>;

    #[method(name = "requestAirdrop")]
    async fn request_airdrop(
        &self,
        pubkey_str: String,
        lamports: u64,
        config: Option<RpcRequestAirdropConfig>,
    ) -> Result<String>;

    // ***********************
    // Direct Subscription Domain
    // ***********************

    #[subscription(name = "programSubscribe" => "programNotification", unsubscribe="programUnsubscribe", item=RpcResponse<serde_json::Value>)]
    async fn program_subscribe(
        &self,
        pubkey_str: String,
        config: Option<RpcProgramAccountsConfig>,
    ) -> SubscriptionResult;

    #[subscription(name = "slotSubscribe" => "slotNotification", unsubscribe="slotUnsubscribe", item=Slot)]
    async fn slot_subscribe(&self) -> SubscriptionResult;

    #[subscription(name = "blockSubscribe" => "blockNotification", unsubscribe="blockUnsubscribe", item=RpcResponse<UiConfirmedBlock>)]
    async fn block_subscribe(
        &self,
        filter: RpcBlockSubscribeFilter,
        config: Option<RpcBlockSubscribeConfig>,
    ) -> SubscriptionResult;

    // [transactionSubscribe](https://github.com/solana-foundation/solana-improvement-documents/pull/69)
    //
    //#[subscription(name = "transactionSubscribe" => "transactionNotification", unsubscribe="transactionUnsubscribe", item=RpcResponse<RpcConfirmedTransactionStatusWithSignature>)]
    //async fn transaction_subscribe(
    //    &self,
    //    commitment_config: CommitmentConfig,
    //) -> SubscriptionResult;

    // ***********************
    // Indirect Subscription Domain
    // ***********************

    #[subscription(name = "logsSubscribe" => "logsNotification", unsubscribe="logsUnsubscribe", item=RpcResponse<RpcLogsResponse>)]
    async fn logs_subscribe(
        &self,
        filter: RpcTransactionLogsFilter,
        config: Option<RpcTransactionLogsConfig>,
    ) -> SubscriptionResult;

    // WARN: enable_received_notification: bool is ignored
    #[subscription(name = "signatureSubscribe" => "signatureNotification", unsubscribe="signatureUnsubscribe", item=RpcResponse<serde_json::Value>)]
    async fn signature_subscribe(
        &self,
        signature: String,
        config: RpcSignatureSubscribeConfig,
    ) -> SubscriptionResult;

    #[subscription(name = "slotUpdatesSubscribe" => "slotUpdatesNotification", unsubscribe="slotUpdatesUnsubscribe", item=SlotUpdate)]
    async fn slot_updates_subscribe(&self) -> SubscriptionResult;

    #[subscription(name = "voteSubscribe" => "voteNotification", unsubscribe="voteUnsubscribe", item=RpcVote)]
    async fn vote_subscribe(&self) -> SubscriptionResult;

    #[method(name = "getEpochInfo")]
    async fn get_epoch_info(
        &self,
        config: Option<RpcContextConfig>,
    ) -> crate::rpc::Result<EpochInfo>;

    #[method(name = "getLeaderSchedule")]
    async fn get_leader_schedule(
        &self,
        slot: Option<u64>,
        config: Option<RpcLeaderScheduleConfig>,
    ) -> crate::rpc::Result<Option<HashMap<String, Vec<usize>>>>;

    #[method(name = "getSlotLeaders")]
    async fn get_slot_leaders(
        &self,
        start_slot: u64,
        limit: u64,
    ) -> crate::rpc::Result<Vec<Pubkey>>;

    #[method(name = "getVoteAccounts")]
    async fn get_vote_accounts(
        &self,
        config: Option<RpcGetVoteAccountsConfig>,
    ) -> crate::rpc::Result<RpcVoteAccountStatus>;

    // ***********************
    // expose prio fees distribution per block
    // (this is special method not available in solana rpc)
    // ***********************

    #[method(name = "getLatestBlockPrioFees")]
    async fn get_latest_block_priofees(&self) -> crate::rpc::Result<RpcResponse<PrioFeesStats>>;

    /// subscribe to prio fees distribution per block; uses confirmation level "confirmed"
    #[subscription(name = "blockPrioFeesSubscribe" => "blockPrioFeesNotification", unsubscribe="blockPrioFeesUnsubscribe", item=PrioritizationFeesStats)]
    async fn latest_block_priofees_subscribe(&self) -> SubscriptionResult;

}
