use async_trait::async_trait;
use solana_lite_rpc_core::commitment_utils::Commitment;
use solana_lite_rpc_core::structures::account_data::AccountData;
use solana_rpc_client_api::filter::RpcFilterType;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::slot_history::Slot;

#[async_trait]
pub trait AccountStorageInterface: Send + Sync {
    async fn update_processed_account(
        &self,
        account_pk: Pubkey,
        account_data: AccountData,
        block_hash: Hash,
    );

    async fn initilize_account(&self, account_pk: Pubkey, account_data: AccountData);

    async fn get_account(&self, account_pk: Pubkey, commitment: Commitment) -> Option<AccountData>;

    async fn get_program_accounts(
        &self,
        program_pubkey: &Pubkey,
        account_filter: &Option<RpcFilterType>,
        commitment: Commitment,
    ) -> Vec<AccountData>;

    async fn process_slot_data(
        &self,
        slot: Slot,
        block_hash: Hash,
        commitment: Commitment,
    ) -> Vec<AccountData>;
}
