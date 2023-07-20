use std::sync::Arc;

use solana_lite_rpc_core::solana_utils::SolanaUtils;
use solana_lite_rpc_core::AtomicSlot;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::broadcast;

use crate::slot_estimator::SlotClock;

const MAX_BLOCK_INDEXERS: usize = 10;

pub struct RpcListener {
    rpc_client: Arc<RpcClient>,
}

impl RpcListener {
    // get's block for the respective slot
    async fn get_block_for_slot(slot: _) -> _ {
        todo!()
    }

    pub async fn listen(
        rpc_client: &RpcClient,
        slots_tx: AtomicSlot,
        block_tx: UnboundedSender<Block>,
    ) -> anyhow::Result<()> {
        while let Some(slot) = rx.recv().await {
            log::trace!(
                "Block indexers running {:?}/MAX_BLOCK_INDEXERS",
                MAX_BLOCK_INDEXERS - block_worker_semaphore.available_permits()
            );

            let permit = block_worker_semaphore.clone().acquire_owned().await?;

            tokio::spawn(async move {
                Self::get_block_for_slot(slot).await.unwrap();

                drop(permit);
            });
        }

        Ok(())
    }
}
