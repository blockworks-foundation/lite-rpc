use std::ops::{Deref, DerefMut};

use solana_client::{nonblocking::rpc_client::RpcClient, rpc_request::RpcRequest};
use solana_sdk::commitment_config::CommitmentConfig;

pub const LOCAL_LIGHT_RPC_ADDR: &str = "http://127.0.0.1:8890";

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
        self.confirm_transaction_with_commitment(signature, self.0.commitment())
            .await
    }

    pub async fn confirm_transaction_with_commitment(
        &self,
        signature: String,
        commitment_config: CommitmentConfig,
    ) -> bool {
        self.send(
            RpcRequest::Custom {
                method: "confirmTransaction",
            },
            serde_json::json!([signature, commitment_config]),
        )
        .await
        .unwrap()
    }
}
