use async_trait::async_trait;
use solana_sdk::{pubkey::Pubkey, slot_history::Slot};

#[derive(Debug, Clone)]
pub struct LeaderData {
    pub leader_slot: Slot,
    pub pubkey: Pubkey,
}

#[async_trait]
pub trait LeaderFetcherInterface: Send + Sync {
    async fn get_slot_leaders(&self, from: Slot, to: Slot) -> anyhow::Result<Vec<LeaderData>>;
}
