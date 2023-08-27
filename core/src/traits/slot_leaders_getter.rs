use async_trait::async_trait;
use solana_sdk::{pubkey::Pubkey, slot_history::Slot};

#[async_trait]
pub trait SlotLeadersGetter: Send + Sync {
    async fn get_slot_leaders(&self, from: Slot, to: Slot) -> anyhow::Result<Vec<Pubkey>>;
}
