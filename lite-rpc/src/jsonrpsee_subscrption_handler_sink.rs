use async_trait::async_trait;
use jsonrpsee::{SubscriptionMessage, SubscriptionSink};
use solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext};

pub struct JsonRpseeSubscriptionHandlerSink {
    jsonrpsee_sink: SubscriptionSink,
}

impl JsonRpseeSubscriptionHandlerSink {
    pub fn new(jsonrpsee_sink: SubscriptionSink) -> Self {
        Self { jsonrpsee_sink }
    }
}

#[async_trait]
impl solana_lite_rpc_core::subscription_sink::SubscriptionSink
    for JsonRpseeSubscriptionHandlerSink
{
    async fn send(&self, slot: solana_sdk::slot_history::Slot, message: serde_json::Value) {
        let _ = self
            .jsonrpsee_sink
            .send(
                SubscriptionMessage::from_json(&RpcResponse {
                    context: RpcResponseContext {
                        slot,
                        api_version: None,
                    },
                    value: message,
                })
                .unwrap(),
            )
            .await;
    }

    fn is_closed(&self) -> bool {
        self.jsonrpsee_sink.is_closed()
    }
}
