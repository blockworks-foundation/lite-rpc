use async_trait::async_trait;
use jsonrpsee::{SubscriptionMessage, SubscriptionSink};
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};

pub struct JsonRpseeSubscriptionHandlerSink(SubscriptionSink);

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
        self.jsonrpsee_sink.is_closed()
    }
}
