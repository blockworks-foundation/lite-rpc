use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use log::info;
use prometheus::{opts, register_int_counter, IntCounter};
use solana_quic_client::{QuicConfig, QuicPool};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use solana_tpu_client::{
    nonblocking::tpu_client::TpuClient,
    tpu_client::TpuClientConfig,
    tpu_connection_cache::{NewTpuConfig, TpuConnectionCache},
};
use tokio::sync::RwLock;

pub type QuicTpuClient = TpuClient<QuicPool>;
pub type QuicConnectionCache = TpuConnectionCache<QuicPool>;

const TPU_CONNECTION_CACHE_SIZE: usize = 4;

lazy_static::lazy_static! {
static ref TPU_CONNECTION_RESET: IntCounter =
    register_int_counter!(opts!("literpc_tpu_connection_reset", "Number of times tpu connection was reseted")).unwrap();
}

#[derive(Clone)]
pub struct TpuManager {
    error_count: Arc<AtomicU32>,
    rpc_client: Arc<RpcClient>,
    // why arc twice / one is so that we clone rwlock and other so that we can clone tpu client
    tpu_client: Arc<RwLock<Arc<QuicTpuClient>>>,
    pub ws_addr: String,
    fanout_slots: u64,
    identity: Arc<Keypair>,
}

impl TpuManager {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        ws_addr: String,
        fanout_slots: u64,
        identity: Keypair,
    ) -> anyhow::Result<Self> {
        let mut tpu_config = QuicConfig::new().unwrap();
        tpu_config
            .update_client_certificate(&identity, IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))
            .unwrap();

        let connection_cache =
            QuicConnectionCache::new_with_config(TPU_CONNECTION_CACHE_SIZE, tpu_config);
        let connection_cache = Arc::new(connection_cache);
        let tpu_client =
            Self::new_tpu_client(rpc_client.clone(), &ws_addr, fanout_slots, connection_cache)
                .await?;
        let tpu_client = Arc::new(RwLock::new(Arc::new(tpu_client)));

        Ok(Self {
            rpc_client,
            tpu_client,
            ws_addr,
            fanout_slots,
            error_count: Default::default(),
            identity: Arc::new(identity),
        })
    }

    pub async fn new_tpu_client(
        rpc_client: Arc<RpcClient>,
        ws_addr: &str,
        fanout_slots: u64,
        connection_cache: Arc<QuicConnectionCache>,
    ) -> anyhow::Result<QuicTpuClient> {
        Ok(TpuClient::new_with_connection_cache(
            rpc_client.clone(),
            ws_addr,
            TpuClientConfig { fanout_slots },
            connection_cache,
        )
        .await?)
    }

    pub async fn reset_tpu_client(&self) -> anyhow::Result<()> {
        let mut tpu_config = QuicConfig::new().unwrap();
        tpu_config
            .update_client_certificate(&self.identity, IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))
            .unwrap();
        let connection_cache =
            QuicConnectionCache::new_with_config(TPU_CONNECTION_CACHE_SIZE, tpu_config);
        let connection_cache = Arc::new(connection_cache);

        let tpu_client = Self::new_tpu_client(
            self.rpc_client.clone(),
            &self.ws_addr,
            self.fanout_slots,
            connection_cache,
        )
        .await?;
        self.error_count.store(0, Ordering::Relaxed);
        *self.tpu_client.write().await = Arc::new(tpu_client);
        TPU_CONNECTION_RESET.inc();
        Ok(())
    }

    pub async fn reset(&self) -> anyhow::Result<()> {
        self.error_count.fetch_add(1, Ordering::Relaxed);

        if self.error_count.load(Ordering::Relaxed) > 5 {
            self.reset_tpu_client().await?;
            info!("TPU Reset after 5 errors");
        }

        Ok(())
    }

    async fn get_tpu_client(&self) -> Arc<QuicTpuClient> {
        self.tpu_client.read().await.clone()
    }

    pub async fn try_send_wire_transaction_batch(
        &self,
        wire_transactions: Vec<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let tpu_client = self.get_tpu_client().await;
        match tpu_client
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

    pub async fn estimated_current_slot(&self) -> u64 {
        let tpu_client = self.get_tpu_client().await;
        tpu_client.estimated_current_slot()
    }
}
