use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use log::info;
use solana_quic_client::QuicPool;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_tpu_client::{nonblocking::tpu_client::TpuClient, tpu_client::TpuClientConfig};
use tokio::sync::{RwLock, RwLockReadGuard};

pub type QuicTpuClient = TpuClient<QuicPool>;

#[derive(Clone)]
pub struct TpuManager {
    error_count: Arc<AtomicU32>,
    rpc_client: Arc<RpcClient>,
    tpu_client: Arc<RwLock<QuicTpuClient>>,
    pub ws_addr: String,
    fanout_slots: u64,
}

impl TpuManager {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        ws_addr: String,
        fanout_slots: u64,
    ) -> anyhow::Result<Self> {
        let tpu_client = Self::new_tpu_client(rpc_client.clone(), &ws_addr, fanout_slots).await?;
        let tpu_client = Arc::new(RwLock::new(tpu_client));

        Ok(Self {
            rpc_client,
            tpu_client,
            ws_addr,
            fanout_slots,
            error_count: Default::default(),
        })
    }

    pub async fn new_tpu_client(
        rpc_client: Arc<RpcClient>,
        ws_addr: &str,
        fanout_slots: u64,
    ) -> anyhow::Result<QuicTpuClient> {
        Ok(TpuClient::new(
            rpc_client.clone(),
            ws_addr,
            TpuClientConfig { fanout_slots },
        )
        .await?)
    }

    pub async fn reset(&self) -> anyhow::Result<()> {
        self.error_count.fetch_add(1, Ordering::Relaxed);

        if self.error_count.load(Ordering::Relaxed) > 5 {
            let tpu_client =
                Self::new_tpu_client(self.rpc_client.clone(), &self.ws_addr, self.fanout_slots)
                    .await?;
            self.error_count.store(0, Ordering::Relaxed);
            *self.tpu_client.write().await = tpu_client;
            info!("TPU Reset after 5 errors");
        }

        Ok(())
    }

    pub async fn try_send_wire_transaction_batch(
        &self,
        wire_transactions: Vec<Vec<u8>>,
    ) -> anyhow::Result<()> {
        match self
            .tpu_client
            .read()
            .await
            .try_send_wire_transaction_batch(wire_transactions)
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                self.reset().await?;
                Err(err.into())
            }
        }
    }

    pub async fn get_tpu_client(&self) -> RwLockReadGuard<QuicTpuClient> {
        self.tpu_client.read().await
    }
}
