use anyhow::bail;
use prometheus::{opts, register_gauge, Gauge};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::sync::Arc;

lazy_static::lazy_static! {
    static ref RPC_RESPONDING: Gauge =
    register_gauge!(opts!("literpc_rpc_responding", "If LiteRpc is responding")).unwrap();
}

pub struct RpcTester {
    rpc_client: Arc<RpcClient>,
}

impl RpcTester {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self { rpc_client }
    }
}

impl RpcTester {
    /// Starts a loop that checks if the rpc is responding every 5 seconds
    pub async fn start(self) -> anyhow::Result<()> {
        let mut error_counter = 0;
        let rpc_client = self.rpc_client;
        loop {
            // sleep for 5 seconds
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            // do a simple request to self for getVersion
            let Err(err) = rpc_client.get_version().await else {
                RPC_RESPONDING.set(1.0);
                continue;
            };

            RPC_RESPONDING.set(0.0);
            error_counter += 1;
            if error_counter > 10 {
                bail!("RPC seems down restarting service error {err:?}");
            }
        }
    }
}
