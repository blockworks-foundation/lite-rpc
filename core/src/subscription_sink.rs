use async_trait::async_trait;
use solana_sdk::slot_history::Slot;

#[async_trait]
pub trait SubscriptionSink: Send + Sync {
    async fn send(&self, slot: Slot, message: serde_json::Value);
    fn is_closed(&self) -> bool;
}
