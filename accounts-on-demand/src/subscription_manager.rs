use async_trait::async_trait;
use prometheus::{opts, register_int_gauge, IntGauge};

use solana_lite_rpc_core::structures::account_filter::AccountFilters;

lazy_static::lazy_static! {
static ref ON_DEMAND_SUBSCRIPTION_RESTARTED: IntGauge =
        register_int_gauge!(opts!("literpc_count_account_on_demand_resubscribe", "Count number of account on demand has resubscribed")).unwrap();

        static ref ON_DEMAND_UPDATES: IntGauge =
        register_int_gauge!(opts!("literpc_count_account_on_demand_updates", "Count number of updates for account on demand")).unwrap();
}

#[async_trait]
pub trait SubscriptionManger: Send + Sync {
    async fn update_subscriptions(&self, _filters: AccountFilters);
}
