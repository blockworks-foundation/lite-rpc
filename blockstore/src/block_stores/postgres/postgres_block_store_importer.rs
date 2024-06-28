use crate::block_stores::postgres::postgres_block_store_writer::PostgresBlockStore;
use log::{debug, info, warn};
use std::cell::OnceCell;

use solana_lite_rpc_core::types::{BlockStream, SlotStream};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::RecvError;

use solana_lite_rpc_core::structures::slot_notification::SlotNotification;
use tokio::task::{AbortHandle, JoinHandle};
use tokio_util::sync::CancellationToken;

const CHANNEL_SIZE_WARNING_THRESHOLD: usize = 5;
/// run the optimizer at least every n slots
const OPTIMIZE_EVERY_N_SLOTS: u64 = 10;
/// wait at least n slots before running the optimizer again
const OPTIMIZE_DEBOUNCE_SLOTS: u64 = 4;

// note: the consumer lags far behind the ingress of blocks and transactions
pub fn start_postgres_block_store_importer_task(
    block_notifier: BlockStream,
    // TODO try to avoid passing arc but keep store locally inside task
    block_storage: Arc<PostgresBlockStore>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("Start block storage importer task");
        let mut block_notifier = block_notifier;
        let first_block_written: OnceCell<Slot> = OnceCell::new();
        // this is the critical write loop
        'recv_loop: loop {
            match block_notifier.recv().await {
                Ok(block) => {
                    if block.commitment_config != CommitmentConfig::confirmed() {
                        debug!(
                            "Skip block {}@{} due to commitment level",
                            block.slot, block.commitment_config.commitment
                        );
                        continue;
                    }
                    let started = Instant::now();
                    debug!(
                        "Storage task received block: {}@{} with {} txs",
                        block.slot,
                        block.commitment_config.commitment,
                        block.transactions.len()
                    );

                    if block_notifier.len() > CHANNEL_SIZE_WARNING_THRESHOLD {
                        warn!(
                            "(soft_realtime) Block queue is growing - {} elements",
                            block_notifier.len()
                        );
                    }

                    // TODO we should intercept finalized blocks and try to update only the status optimistically

                    // avoid backpressure here!

                    match block_storage.save_confirmed_block(&block).await {
                        Ok(_ok) => {}
                        Err(err) => {
                            warn!(
                                "Error saving block {}@{} to postgres: {:?}",
                                block.slot, block.commitment_config.commitment, err
                            );
                            continue 'recv_loop;
                        }
                    }

                    // we should be faster than 150ms here
                    let elapsed = started.elapsed();
                    debug!(
                        "Successfully stored block {} to postgres which took {:.2}ms",
                        block.slot,
                        elapsed.as_secs_f64() * 1000.0);
                    if elapsed > Duration::from_millis(250) {
                        warn!("(soft_realtime) Write operation was slow ({:.2?})!", elapsed);
                    }

                    if first_block_written.set(block.slot).is_err() {
                        info!("Stored first block {} after restart!", block.slot);
                    }

                    block_storage
                        .optimize_tables(block.slot)
                        .await
                        .unwrap();

                } // -- Ok
                Err(RecvError::Lagged(missed_blocks)) => {
                    warn!(
                        "Could not keep up with producer - missed {} blocks",
                        missed_blocks
                    );
                }
                Err(RecvError::Closed) => {
                    warn!("Error receiving block as source channel was closed - aborting");
                    break 'recv_loop;
                }
            }

            // ...
        } // -- END recv_loop
    })
}

pub fn storage_prepare_epoch_schema(
    slot_notifier: SlotStream,
    postgres_storage: Arc<PostgresBlockStore>,
) -> (AbortHandle, CancellationToken) {
    let mut debounce_slot = 0;
    let building_epoch_schema = CancellationToken::new();
    let first_run_signal = building_epoch_schema.clone();
    let join_handle = tokio::spawn(async move {
        let mut slot_notifier = slot_notifier;
        loop {
            match slot_notifier.recv().await {
                Ok(SlotNotification { processed_slot, .. }) => {
                    if processed_slot >= debounce_slot {
                        let created = postgres_storage
                            .prepare_epoch_schema(processed_slot)
                            .await
                            .unwrap();
                        first_run_signal.cancel();
                        debounce_slot = processed_slot + 64; // wait a bit before hammering the DB again
                        if created {
                            debug!("Async job prepared schema at slot {}", processed_slot);
                        } else {
                            debug!(
                                "Async job for preparing schema at slot {} was a noop",
                                processed_slot
                            );
                        }
                    }
                }
                _ => {
                    warn!("Error receiving slot - continue");
                }
            }
        }
    });
    (join_handle.abort_handle(), building_epoch_schema)
}
