use async_trait::async_trait;
use jsonrpsee::{SubscriptionMessage, SubscriptionSink};

pub struct JsonRpseeSubscriptionHandlerSink(pub SubscriptionSink);

#[async_trait]
impl solana_lite_rpc_core::subscription_sink::SubscriptionSink
    for JsonRpseeSubscriptionHandlerSink
{
    async fn send(&self, message: &serde_json::Value) {
        let _ = self
            .0
            .send(SubscriptionMessage::from_json(message).unwrap())
            .await;
    }

    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}
