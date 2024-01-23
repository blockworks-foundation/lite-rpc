use crate::rpc_data::{PrioFeesStats, PrioFeesUpdateMessage, TxAggregateStats};
use crate::stats_calculation::calculate_supp_percentiles;
use log::{error, info, trace, warn};
use solana_lite_rpc_core::types::BlockStream;
use solana_sdk::clock::Slot;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError::{Closed, Lagged};
use tokio::sync::broadcast::Sender;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

// note: ATM only the latest slot (highest key) is used
const SLOTS_TO_RETAIN: u64 = 100;

/// put everything required to serve sync data calls here
#[derive(Clone)]
pub struct PrioFeeStore {
    // store priofees stats for recently processed blocks up to CLEANUP_SLOTS_AFTER
    recent: Arc<RwLock<BTreeMap<Slot, PrioFeesStats>>>,
}

pub struct PrioFeesService {
    pub block_fees_store: PrioFeeStore,
    // use .subscribe() to get a receiver
    pub block_fees_stream: Sender<PrioFeesUpdateMessage>,
}

impl PrioFeesService {
    pub async fn get_latest_priofees(&self) -> Option<(Slot, PrioFeesStats)> {
        let lock = self.block_fees_store.recent.read().await;
        let latest_in_store = lock.last_key_value();
        latest_in_store.map(|x| (*x.0, x.1.clone()))
    }
}

pub async fn start_block_priofees_task(
    mut block_stream: BlockStream,
) -> (JoinHandle<()>, PrioFeesService) {
    let recent_data = Arc::new(RwLock::new(BTreeMap::new()));
    let store = PrioFeeStore {
        recent: recent_data.clone(),
    };
    let (priofees_update_sender, _priofees_update_receiver) = tokio::sync::broadcast::channel(64);
    let sender_to_return = priofees_update_sender.clone();

    let jh_priofees_task = tokio::spawn(async move {
        let sender = priofees_update_sender.clone();
        'recv_loop: loop {
            let block = block_stream.recv().await;
            match block {
                Ok(block) => {
                    if !block.commitment_config.is_processed() {
                        continue;
                    }
                    let processed_slot = block.slot;
                    {
                        // first do some cleanup
                        let mut lock = recent_data.write().await;
                        lock.retain(|slot, _| *slot > processed_slot - SLOTS_TO_RETAIN);
                    }

                    let block_priofees = block
                        .transactions
                        .iter()
                        .filter(|tx| !tx.is_vote)
                        .map(|tx| {
                            (
                                tx.prioritization_fees.unwrap_or_default(),
                                tx.cu_consumed.unwrap_or_default(),
                            )
                        })
                        .collect::<Vec<(u64, u64)>>();

                    let priofees_percentiles = calculate_supp_percentiles(&block_priofees);

                    let total_tx_count = block.transactions.len() as u64;

                    let nonvote_tx_count = block.transactions.iter()
                        .filter(|tx| !tx.is_vote).count() as u64;

                    let total_cu_consumed = block.transactions.iter()
                        .map(|tx| tx.cu_consumed.unwrap_or(0))
                        .sum::<u64>();

                    let nonvote_cu_consumed = block.transactions.iter()
                        .filter(|tx| !tx.is_vote)
                        .map(|tx| tx.cu_consumed.unwrap_or(0))
                        .sum::<u64>();

                    trace!("Got prio fees stats for processed block {}", processed_slot);

                    let priofees_stats = PrioFeesStats {
                        by_tx: priofees_percentiles.by_tx,
                        by_tx_percentiles: priofees_percentiles.by_tx_percentiles,
                        by_cu: priofees_percentiles.by_cu,
                        by_cu_percentiles: priofees_percentiles.by_cu_percentiles,
                        tx_count: TxAggregateStats {
                            total: total_tx_count,
                            nonvote: nonvote_tx_count,
                        },
                        cu_consumed: TxAggregateStats {
                            total: total_cu_consumed,
                            nonvote: nonvote_cu_consumed,
                        },
                    };

                    {
                        // first do some cleanup
                        let mut lock = recent_data.write().await;
                        lock.insert(processed_slot, priofees_stats.clone());
                    }
                    let msg = PrioFeesUpdateMessage {
                        slot: processed_slot,
                        priofees_stats,
                    };
                    let send_result = sender.send(msg);
                    match send_result {
                        Ok(n_subscribers) => {
                            trace!(
                                "sent priofees update message to {} subscribers (buffer={})",
                                n_subscribers,
                                sender.len()
                            );
                        }
                        Err(_) => {
                            trace!("no subscribers for priofees update message");
                        }
                    }
                }
                Err(Lagged(_lagged)) => {
                    warn!("channel error receiving block for priofees calculation - continue");
                    continue 'recv_loop;
                }
                Err(Closed) => {
                    error!("failed to receive block, sender closed - aborting");
                    break 'recv_loop;
                }
            }
        }
        info!("priofees task shutting down");
    });

    (
        jh_priofees_task,
        PrioFeesService {
            block_fees_store: store,
            block_fees_stream: sender_to_return,
        },
    )
}
