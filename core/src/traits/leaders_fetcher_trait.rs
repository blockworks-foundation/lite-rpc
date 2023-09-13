use crate::structures::leader_data::LeaderData;
use async_trait::async_trait;
use solana_sdk::slot_history::Slot;

#[async_trait]
pub trait LeaderFetcherInterface: Send + Sync {
    async fn get_slot_leaders(&self, from: Slot, to: Slot) -> anyhow::Result<Vec<LeaderData>>;
}
