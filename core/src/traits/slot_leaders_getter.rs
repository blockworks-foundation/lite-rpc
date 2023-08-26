use async_trait::async_trait;
use solana_sdk::{slot_history::Slot, pubkey::Pubkey};

#[async_trait]
pub trait SlotLeadersGetter : Send + Sync {
    async fn get_slot_leaders(&self, from: Slot, to: Slot) -> anyhow::Result<Vec<Pubkey>>;
}