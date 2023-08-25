use std::time::Duration;

use solana_lite_rpc_core::data_cache::DataCache;

//use prometheus::{core::GenericGauge, opts, register_int_gauge};
//lazy_static::lazy_static! {
//    static ref BLOCKS_IN_BLOCKSTORE: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_blocks_in_blockstore", "Number of blocks in blockstore")).unwrap();
//}

/// Background worker which cleans up memory  
#[derive(Clone)]
pub struct Cleaner {
    pub ledger: DataCache,
}

impl Cleaner {
    pub async fn start(self, ttl_duration: Duration) -> anyhow::Result<()> {
        let mut ttl = tokio::time::interval(ttl_duration);

        loop {
            ttl.tick().await;

            log::info!("Cleaning memory");
            self.ledger.clean(ttl_duration).await;
        }
    }
}
