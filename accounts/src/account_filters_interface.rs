use async_trait::async_trait;
use solana_lite_rpc_core::structures::account_data::AccountData;

#[async_trait]
pub trait AccountFiltersStoreInterface: Send + Sync {
    async fn satisfies(&self, account_data: &AccountData) -> bool;
}
