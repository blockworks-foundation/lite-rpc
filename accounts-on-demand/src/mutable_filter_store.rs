use async_trait::async_trait;
use solana_lite_rpc_accounts::{
    account_filters_interface::AccountFiltersStoreInterface, simple_filter_store::SimpleFilterStore,
};
use solana_lite_rpc_core::structures::{
    account_data::AccountData,
    account_filter::{AccountFilter, AccountFilters},
};
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Default)]
pub struct MutableFilterStore {
    filter_store: Arc<RwLock<SimpleFilterStore>>,
}

impl MutableFilterStore {
    pub async fn add_account_filters(&self, account_filters: &AccountFilters) {
        let mut lk = self.filter_store.write().await;
        lk.add_account_filters(account_filters)
    }

    pub async fn contains_account(&self, account_pk: Pubkey) -> bool {
        let lk = self.filter_store.read().await;
        lk.contains_account(account_pk)
    }

    pub async fn contains_filter(&self, account_filter: &AccountFilter) -> bool {
        let lk = self.filter_store.read().await;
        lk.contains_filter(account_filter)
    }

    pub async fn satisfies_filter(&self, account: &AccountData) -> bool {
        let lk = self.filter_store.read().await;
        lk.satisfies_filter(account)
    }
}

#[async_trait]
impl AccountFiltersStoreInterface for MutableFilterStore {
    async fn satisfies(&self, account_data: &AccountData) -> bool {
        self.satisfies_filter(account_data).await
    }
}
