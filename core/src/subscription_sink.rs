use async_trait::async_trait;

#[async_trait]
pub trait SubscriptionSink: Send + Sync {
    async fn send(&self, message: &serde_json::Value);
    fn is_closed(&self) -> bool;
}
