use log::{debug, info, warn};
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::types::BlockStream;


pub fn block_debug_listen(mut block_notifier: BlockStream) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_highest_slot_number = 0;

        loop {
            match block_notifier.recv().await {
                Ok(block) => {
                    debug!(
                        "Saw block: {} @ {} with {} txs",
                        block.slot,
                        block.commitment_config.commitment,
                        block.transactions.len()
                    );

                    // check monotony
                    // note: this succeeds if poll_block parallelism is 1 (see NUM_PARALLEL_BLOCKS)
                    if block.commitment_config == CommitmentConfig::confirmed() {
                        if block.slot > last_highest_slot_number {
                            last_highest_slot_number = block.slot;
                        } else {
                            // note: ATM this fails very often (using the RPC poller)
                            warn!(
                                "Monotonic check failed - block {} is out of order, last highest was {}",
                                block.slot, last_highest_slot_number
                            );
                        }
                    }
                } // -- Ok
                Err(RecvError::Lagged(missed_blocks)) => {
                    warn!(
                        "Could not keep up with producer - missed {} blocks",
                        missed_blocks
                    );
                }
                Err(other_err) => {
                    panic!("Error receiving block: {:?}", other_err);
                }
            }

            // ...
        }
    })
}

