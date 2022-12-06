use std::ops::{Deref, DerefMut};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_request::RpcRequest;

pub struct LiteClient(pub RpcClient);

impl Deref for LiteClient {
    type Target = RpcClient;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for LiteClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl LiteClient {
    pub async fn confirm_transaction(&self, signature: String) -> bool {
        self.send(
            RpcRequest::Custom {
                method: "confirmTransaction",
            },
            serde_json::json!([signature]),
        )
        .await
        .unwrap()
    }
}
