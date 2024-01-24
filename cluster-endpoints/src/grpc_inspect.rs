use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use solana_lite_rpc_core::types::BlockStream;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tokio::time::sleep;


// note: we assume that the invariants hold even right after startup
pub fn debugtask_blockstream_confirmation_sequence(mut block_notifier: BlockStream) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut cleanup_before_slot = 0;
        // throttle cleanup
        let mut slots_since_last_cleanup = 0;
        // use blockhash as key instead of slot as for processed the slot is ambiguous
        let mut saw_processed_at: HashMap<String, (Slot, SystemTime)> = HashMap::new();
        let mut saw_confirmed_at: HashMap<String, (Slot, SystemTime)> = HashMap::new();
        let mut saw_finalized_at: HashMap<String, (Slot, SystemTime)> = HashMap::new();
        'recv_loop: loop {
            match block_notifier.recv().await {
                Ok(block) => {
                    let slot = block.slot;
                    let blockhash = block.blockhash.to_string();
                    if slot < cleanup_before_slot {
                        continue 'recv_loop;
                    }
                    debug!(
                        "Saw block: {}#{} @ {} with {} txs",
                        slot,
                        blockhash,
                        block.commitment_config.commitment,
                        block.transactions.len()
                    );

                    if block.commitment_config.is_processed() {
                        let prev_value = saw_processed_at.insert(blockhash.clone(), (slot, SystemTime::now()));
                        match prev_value {
                            None => {
                                // okey
                            }
                            Some(prev) => {
                                // this is actually fatal
                                error!(
                                    "should not see same processed slot twice ({}) - saw at {:?}",
                                    blockhash, format_timestamp(&prev.1));
                            }
                        }
                    }
                    if block.commitment_config.is_confirmed() {
                        let prev_value = saw_confirmed_at.insert(blockhash.clone(), (slot, SystemTime::now()));
                        match prev_value {
                            None => {
                                // okey
                            }
                            Some(prev) => {
                                // this is actually fatal
                                error!(
                                    "should not see same confirmed slot twice ({}) - saw at {:?}",
                                    blockhash, format_timestamp(&prev.1)
                                );
                            }
                        }
                    }
                    if block.commitment_config.is_finalized() {
                        let prev_value = saw_finalized_at.insert(blockhash.clone(), (slot, SystemTime::now()));
                        match prev_value {
                            None => {
                                // okey
                            }
                            Some(prev) => {
                                // this is actually fatal
                                error!(
                                    "should not see same finalized slot twice ({}) - saw at {:?}",
                                    slot, format_timestamp(&prev.1)
                                );
                            }
                        }
                    }

                    // rule: if confirmed, we should have seen processed but not finalized
                    if block.commitment_config.is_confirmed() {
                        if saw_processed_at.contains_key(&blockhash) {
                            // okey
                        } else {
                            error!("should not see confirmed slot without seeing processed slot first ({})", blockhash);
                        }
                        if saw_finalized_at.contains_key(&blockhash) {
                            error!(
                                "should not see confirmed slot after seeing finalized slot ({})",
                                blockhash
                            );
                        } else {
                            // okey
                        }
                    }

                    // rule: if processed, we should have seen neither confirmed nor finalized
                    if block.commitment_config.is_processed() {
                        if saw_confirmed_at.contains_key(&blockhash) {
                            error!(
                                "should not see processed slot after seeing confirmed slot ({})",
                                blockhash
                            );
                        } else {
                            // okey
                        }
                        if saw_finalized_at.contains_key(&blockhash) {
                            error!(
                                "should not see processed slot after seeing finalized slot ({})",
                                blockhash
                            );
                        } else {
                            // okey
                        }
                    }

                    // rule: if finalized, we should have seen processed and confirmed
                    if block.commitment_config.is_finalized() {
                        if saw_processed_at.contains_key(&blockhash) {
                            // okey
                        } else {
                            error!("should not see finalized slot without seeing processed slot first ({})", blockhash);
                        }
                        if saw_confirmed_at.contains_key(&blockhash) {
                            // okey
                        } else {
                            error!("should not see finalized slot without seeing confirmed slot first ({})", blockhash);
                        }

                        if let (Some(processed), Some(confirmed)) = (
                            saw_processed_at.get(&blockhash),
                            saw_confirmed_at.get(&blockhash),
                        ) {
                            let finalized = saw_finalized_at.get(&blockhash).unwrap();
                            debug!(
                                "block sequence seen on channel for block {} (slot {}): {:?} -> {:?} -> {:?}",
                                blockhash, slot,
                                format_timestamp(&processed.1),
                                format_timestamp(&confirmed.1),
                                format_timestamp(&finalized.1)
                            );
                        }
                    }

                    if slots_since_last_cleanup < 500 {
                        slots_since_last_cleanup += 1;
                    } else {
                        // perform cleanup, THEN update cleanup_before_slot
                        saw_processed_at.retain(|_blockhash, (slot, _instant)| *slot >= cleanup_before_slot);
                        saw_confirmed_at.retain(|_blockhash, (slot, _instant)| *slot >= cleanup_before_slot);
                        saw_finalized_at.retain(|_blockhash, (slot, _instant)| *slot >= cleanup_before_slot);
                        cleanup_before_slot = slot - 200;
                        debug!("move cleanup point to {}", cleanup_before_slot);
                        debug!(
                            "map sizes after cleanup: {} processed, {} confirmed, {} finalized",
                            saw_processed_at.len(),
                            saw_confirmed_at.len(),
                            saw_finalized_at.len()
                        );
                        slots_since_last_cleanup = 0;
                    }
                } // -- Ok
                Err(RecvError::Lagged(missed_blocks)) => {
                    warn!(
                        "Could not keep up with producer - missed {} blocks",
                        missed_blocks
                    );
                }
                Err(other_err) => {
                    error!("Error receiving block: {:?}", other_err);
                    // throttle a bit
                    sleep(Duration::from_millis(1000)).await;
                }
            }

            // ...
        }
    })
}

pub fn debugtask_blockstream_monotonic(
    mut block_notifier: BlockStream,
    commitment_config: CommitmentConfig,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_highest_slot_number = 0;
        let mut last_blockhash: Option<String> = None;

        'recv_loop: loop {
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
                                "parent block@{} is correct ({} -> {})",
                                commitment_config.commitment, block.slot, block.parent_slot
                            );
                        } else {
                            warn!(
                                "parent block@{} not correct ({} -> {}, last_highest_slot_number={})",
                                commitment_config.commitment, block.slot, block.parent_slot, last_highest_slot_number
                            );
                        }
                    }

                    if block.slot > last_highest_slot_number {
                        last_highest_slot_number = block.slot;
                    } else {
                        // note: ATM this fails very often (using the RPC poller)
                        warn!("monotonic check failed - block {} is out of order, last highest was {}",
                            block.slot, last_highest_slot_number
                        );
                    }

                    if let Some(last_blockhash) = last_blockhash {
                        if block.previous_blockhash == last_blockhash {
                            debug!(
                                "parent blockhash for block {} is correct ({} -> {})",
                                commitment_config.commitment, block.blockhash, block.previous_blockhash
                            );
                        } else {
                            warn!("parent blockhash for block {} not correct ({} -> {}, last_blockhash={})",
                                block.slot, block.blockhash, block.previous_blockhash, last_blockhash);
                        }
                    }
                    last_blockhash = Some(block.blockhash.to_string());


                } // -- Ok
                Err(RecvError::Lagged(missed_blocks)) => {
                    // very unlikely to happen
                    warn!("Could not keep up with producer - missed {} blocks",
                        missed_blocks
                    );
                }
                Err(RecvError::Closed) => {
                    info!("Channel was closed - aborting");
                    break 'recv_loop;
                }
            }
        } // -- END receiver loop
        info!("Geyser channel debug task shutting down.")
    })
}

/// e.g. "2024-01-22 11:49:07.173523000"
fn format_timestamp(d: &SystemTime) -> String {
    let datetime = DateTime::<Utc>::from(*d);
    datetime.format("%Y-%m-%d %H:%M:%S.%f").to_string()
}
