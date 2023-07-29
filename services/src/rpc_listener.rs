use std::sync::{atomic::Ordering, Arc};

use solana_lite_rpc_core::{
    block_store::{Block, BlockStore},
    jsonrpc_client::{JsonRpcClient, ProcessedBlock},
    solana_utils::SolanaUtils,
    AtomicSlot,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};
use tokio::sync::{broadcast, mpsc::UnboundedSender};

use crate::{slot_clock::SlotClock, slot_estimator::SlotClock};

const MAX_BLOCK_INDEXERS: usize = 10;

#[derive(Clone)]
pub struct RpcListener {
    rpc_client: Arc<RpcClient>,
}

impl RpcListener {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self { rpc_client }
    }

    // get's block for the respective slot
    #[inline]
    async fn process_slot(
        &self,
        slot: Slot,
        atomic_current_slot: AtomicSlot,
        commitment_config: CommitmentConfig,
        block_tx: UnboundedSender<ProcessedBlock>,
    ) -> anyhow::Result<()> {
        // retry the slot till it is at most 128 slots behind the current slot
        while atomic_current_slot
            .load(Ordering::Relaxed)
            .saturating_sub(slot)
            > 10
        {
            let Ok(processed_block) = JsonRpcClient::process(&self.rpc_client, slot, commitment_config)? else {
                // retry after 10ms
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            };

            // send the processed block
            block_tx.send(processed_block)?;
        }
    }

    pub async fn listen(
        self,
        slot_rx: broadcast::Receiver<Slot>,
        block_tx: UnboundedSender<ProcessedBlock>,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<()> {
        let curernt_slot = AtomicSlot::default();

        while let Some(slot) = slot_rx.recv().await {
            // update the current slot for retry queue
            curernt_slot.store(slot, Ordering::Relaxed);

            log::trace!(
                "Block indexers running {:?}/MAX_BLOCK_INDEXERS",
                MAX_BLOCK_INDEXERS - block_worker_semaphore.available_permits()
            );

            let permit = block_worker_semaphore.clone().acquire_owned().await?;
            let block_tx = block_tx.clone();

            tokio::spawn(async {
                self.process_slot(slot, curernt_slot, commitment_config, block_tx)
                    .await
                    .unwrap();

                drop(permit);
            });
        }

        Ok(())
    }
}
