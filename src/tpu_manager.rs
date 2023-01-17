
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::nonblocking::tpu_client::TpuClient;
use solana_client::tpu_client::TpuClientConfig;

pub struct TpuManager {
    error_count: AtomicU32,
    tpu_client: TpuClient,
    ws_addr: String,
    fanout_slots: u64,
}

impl TpuManager {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        ws_addr: String,
        fanout_slots: u64,
    ) -> anyhow::Result<Self> {
        let tpu_client = TpuClient::new(
            rpc_client.clone(),
            &ws_addr,
            TpuClientConfig { fanout_slots },
        )
        .await?;

        Ok(Self {
            error_count: Default::default(),
            tpu_client,
            ws_addr,
            fanout_slots,
        })
    }

    pub async fn try_send_wire_transaction_batch(
        &self,
        wire_transactions: Vec<Vec<u8>>,
    ) -> anyhow::Result<()> {
        match self
            .tpu_client
            .try_send_wire_transaction_batch(wire_transactions)
            .await
        {
            Ok(ok) => Ok(ok),
            Err(err) => {
                self.error_count.fetch_add(1, Ordering::Relaxed);
                Err(err.into())
            }
        }
    }

    pub fn tpu_client(&self) -> &TpuClient {
        &self.tpu_client
    }
}
