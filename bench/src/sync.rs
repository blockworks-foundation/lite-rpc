use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::hash::Hash;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct SyncService {
    slot: Arc<AtomicU64>,
    bh: Arc<RwLock<Hash>>,
}

impl SyncService {
    pub async fn new(rpc_client: &RpcClient) -> anyhow::Result<Self> {
        let (slot, bh) = Self::get_data(rpc_client).await?;

        Ok(Self {
            slot: Arc::new(AtomicU64::new(slot)),
            bh: Arc::new(RwLock::new(bh)),
        })
    }

    pub fn get_slot(&self) -> u64 {
        self.slot.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub async fn get_blockhash(&self) -> Hash {
        *self.bh.read().await
    }

    async fn get_data(rpc_client: &RpcClient) -> anyhow::Result<(u64, Hash)> {
        let bh = rpc_client.get_latest_blockhash().await?;
        let slot = rpc_client.get_slot().await?;

        Ok((slot, bh))
    }

    pub async fn sync(self, rpc_client: Arc<RpcClient>) -> anyhow::Result<()> {
        loop {
            let (slot, bh) = Self::get_data(&rpc_client).await?;

            self.slot.store(slot, std::sync::atomic::Ordering::Relaxed);
            *self.bh.write().await = bh;

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
