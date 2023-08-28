use std::sync::Arc;

use solana_lite_rpc_core::{
    jsonrpc_client::JsonRpcClient, processed_block::ProcessedBlock, slot_clock::SlotClock,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    Semaphore,
};

const MAX_BLOCK_INDEXERS: usize = 1000;

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
        slot_clock: SlotClock,
        commitment_config: CommitmentConfig,
        block_tx: UnboundedSender<ProcessedBlock>,
    ) -> anyhow::Result<()> {
        // retry the slot till it is at most 128 slots behind the current slot
        while slot_clock.get_current_slot().saturating_sub(slot) < 128 {
            let Ok(Ok(processed_block)) =
                JsonRpcClient::process(&self.rpc_client, slot, commitment_config).await
            else {
                // retry after 10ms
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            };

            // send the processed block
            block_tx.send(processed_block)?;
        }

        Ok(())
    }

    pub async fn listen(
        self,
        slot_clock: SlotClock,
        mut slot_rx: UnboundedReceiver<Slot>,
        block_tx: UnboundedSender<ProcessedBlock>,
    ) -> anyhow::Result<()> {
        let block_worker_semaphore = Arc::new(Semaphore::new(MAX_BLOCK_INDEXERS));

        loop {
            let slot = slot_clock.subscribe_reciever(&mut slot_rx).await;
            log::trace!("Processing slot: {}", slot);

            log::info!(
                "Block indexers running {:?}/{MAX_BLOCK_INDEXERS}",
                MAX_BLOCK_INDEXERS - block_worker_semaphore.available_permits()
            );

            let permit = block_worker_semaphore.clone().acquire_owned().await?;
            let block_tx = block_tx.clone();

            let this = self.clone();
            let slot_clock = slot_clock.clone();

            tokio::spawn(async move {
                let confirmed = this.process_slot(
                    slot,
                    slot_clock.clone(),
                    CommitmentConfig::confirmed(),
                    block_tx.clone(),
                );

                let finalized =
                    this.process_slot(slot, slot_clock, CommitmentConfig::finalized(), block_tx);

                let (confirmed_res, finalized_err) = tokio::join!(confirmed, finalized);

                if let Err(err) = confirmed_res {
                    log::error!("Error processing confirmed block: {err:?}");
                }

                if let Err(err) = finalized_err {
                    log::error!("Error processing finalized block: {err:?}");
                }

                drop(permit);
            });
        }
    }
}
