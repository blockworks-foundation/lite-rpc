use log::{debug, warn};
use solana_lite_rpc_blocks_processing::produced_block::BlockStream;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

pub fn block_debug_listen(
    mut block_notifier: BlockStream,
    commitment_config: CommitmentConfig,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_highest_slot_number = 0;

        loop {
            match block_notifier.recv().await {
                Ok(block) => {
                    if block.commitment_config != commitment_config {
                        continue;
                    }

                    debug!(
                        "Saw block: {} @ {} with {} txs",
                        block.slot,
                        block.commitment_config.commitment,
                        block.transactions.len()
                    );

                    if last_highest_slot_number != 0 {
                        if block.parent_slot == last_highest_slot_number {
                            debug!(
                                "parent slot is correct ({} -> {})",
                                block.slot, block.parent_slot
                            );
                        } else {
                            warn!(
                                "parent slot not correct ({} -> {})",
                                block.slot, block.parent_slot
                            );
                        }
                    }

                    if block.slot > last_highest_slot_number {
                        last_highest_slot_number = block.slot;
                    } else {
                        // note: ATM this fails very often (using the RPC poller)
                        warn!(
                            "Monotonic check failed - block {} is out of order, last highest was {}",
                            block.slot, last_highest_slot_number
                        );
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
