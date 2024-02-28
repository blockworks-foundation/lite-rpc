use crate::configs::{IsBlockHashValidConfig, SendTransactionConfig};
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use solana_account_decoder::UiAccount;
use solana_lite_rpc_prioritization_fees::prioritization_fee_calculation_method::PrioritizationFeeCalculationMethod;
use solana_lite_rpc_prioritization_fees::rpc_data::{AccountPrioFeesStats, PrioFeesStats};
use solana_rpc_client_api::config::{
    RpcAccountInfoConfig, RpcBlocksConfigWrapper, RpcContextConfig, RpcGetVoteAccountsConfig,
    RpcLeaderScheduleConfig, RpcProgramAccountsConfig, RpcRequestAirdropConfig,
    RpcSignatureStatusConfig, RpcSignaturesForAddressConfig,
};
use solana_rpc_client_api::response::{
    OptionalContext, Response as RpcResponse, RpcBlockhash,
    RpcConfirmedTransactionStatusWithSignature, RpcContactInfo, RpcKeyedAccount, RpcPerfSample,
    RpcPrioritizationFee, RpcVersionInfo, RpcVoteAccountStatus,
};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::slot_history::Slot;
use solana_transaction_status::{TransactionStatus, UiConfirmedBlock};
use std::collections::HashMap;

#[rpc(server)]
pub trait LiteRpc {
    // ***********************
    // History Domain
    // ***********************

    #[method(name = "getBlock")]
    async fn get_block(&self, slot: u64) -> RpcResult<Option<UiConfirmedBlock>>;

    #[method(name = "getBlocks")]
    async fn get_blocks(
        &self,
        start_slot: Slot,
        config: Option<RpcBlocksConfigWrapper>,
        commitment: Option<CommitmentConfig>,
    ) -> RpcResult<Vec<Slot>>;

    #[method(name = "getSignaturesForAddress")]
    async fn get_signatures_for_address(
        &self,
        address: String,
        config: Option<RpcSignaturesForAddressConfig>,
    ) -> RpcResult<Vec<RpcConfirmedTransactionStatusWithSignature>>;

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
    async fn get_cluster_nodes(&self) -> RpcResult<Vec<RpcContactInfo>>;

    // ***********************
    // Validator Domain
    // ***********************

    #[method(name = "getSlot")]
    async fn get_slot(&self, config: Option<RpcContextConfig>) -> RpcResult<Slot>;

    #[method(name = "getBlockHeight")]
    async fn get_block_height(&self, config: Option<RpcContextConfig>) -> RpcResult<u64>;

    #[method(name = "getBlockTime")]
    async fn get_block_time(&self, block: u64) -> RpcResult<u64>;

    #[method(name = "getFirstAvailableBlock")]
    async fn get_first_available_block(&self) -> RpcResult<u64>;

    #[method(name = "getLatestBlockhash")]
    async fn get_latest_blockhash(
        &self,
        config: Option<RpcContextConfig>,
    ) -> RpcResult<RpcResponse<RpcBlockhash>>;

    #[method(name = "isBlockhashValid")]
    async fn is_blockhash_valid(
        &self,
        blockhash: String,
        config: Option<IsBlockHashValidConfig>,
    ) -> RpcResult<RpcResponse<bool>>;

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
    ) -> RpcResult<Vec<RpcPerfSample>>;

    #[method(name = "getSignatureStatuses")]
    async fn get_signature_statuses(
        &self,
        signature_strs: Vec<String>,
        config: Option<RpcSignatureStatusConfig>,
    ) -> RpcResult<RpcResponse<Vec<Option<TransactionStatus>>>>;

    #[method(name = "getRecentPrioritizationFees")]
    async fn get_recent_prioritization_fees(
        &self,
        pubkey_strs: Vec<String>,
    ) -> RpcResult<Vec<RpcPrioritizationFee>>;

    // ***********************
    // Send Transaction Domain
    // ***********************

    #[method(name = "sendTransaction")]
    async fn send_transaction(
        &self,
        tx: String,
        send_transaction_config: Option<SendTransactionConfig>,
    ) -> RpcResult<String>;

    // ***********************
    // Deprecated
    // ***********************

    #[method(name = "getVersion")]
    fn get_version(&self) -> RpcResult<RpcVersionInfo>;

    #[method(name = "requestAirdrop")]
    async fn request_airdrop(
        &self,
        pubkey_str: String,
        lamports: u64,
        config: Option<RpcRequestAirdropConfig>,
    ) -> RpcResult<String>;

    // **********************

    #[method(name = "getEpochInfo")]
    async fn get_epoch_info(&self, config: Option<RpcContextConfig>) -> RpcResult<EpochInfo>;

    #[method(name = "getLeaderSchedule")]
    async fn get_leader_schedule(
        &self,
        slot: Option<u64>,
        config: Option<RpcLeaderScheduleConfig>,
    ) -> RpcResult<Option<HashMap<String, Vec<usize>>>>;

    #[method(name = "getSlotLeaders")]
    async fn get_slot_leaders(&self, start_slot: u64, limit: u64) -> RpcResult<Vec<Pubkey>>;

    #[method(name = "getVoteAccounts")]
    async fn get_vote_accounts(
        &self,
        config: Option<RpcGetVoteAccountsConfig>,
    ) -> RpcResult<RpcVoteAccountStatus>;

    // ***********************
    // expose prio fees distribution per block
    // (this is special method not available in solana rpc)
    // ***********************

    #[method(name = "getLatestBlockPrioFees")]
    async fn get_latest_block_priofees(
        &self,
        method: Option<PrioritizationFeeCalculationMethod>,
    ) -> RpcResult<RpcResponse<PrioFeesStats>>;

    #[method(name = "getLatestAccountPrioFees")]
    async fn get_latest_account_priofees(
        &self,
        account: String,
        method: Option<PrioritizationFeeCalculationMethod>,
    ) -> RpcResult<RpcResponse<AccountPrioFeesStats>>;

    // **************************
    // Accounts
    // **************************

    #[method(name = "getAccountInfo")]
    async fn get_account_info(
        &self,
        pubkey_str: String,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Option<UiAccount>>>;

    #[method(name = "getMultipleAccounts")]
    async fn get_multiple_accounts(
        &self,
        pubkey_strs: Vec<String>,
        config: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<RpcResponse<Vec<Option<UiAccount>>>>;

    #[method(name = "getProgramAccounts")]
    async fn get_program_accounts(
        &self,
        program_id_str: String,
        config: Option<RpcProgramAccountsConfig>,
    ) -> RpcResult<OptionalContext<Vec<RpcKeyedAccount>>>;

    #[method(name = "getBalance")]
    async fn get_balance(
        &self,
        pubkey_str: String,
        config: Option<RpcContextConfig>,
    ) -> RpcResult<RpcResponse<u64>>;
}
