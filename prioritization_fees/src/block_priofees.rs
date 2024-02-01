use crate::prioritization_fee_data::{BlockPrioData, PrioFeesData};
use crate::rpc_data::{PrioFeesStats, PrioFeesUpdateMessage};
use log::{error, info, trace, warn};
use solana_lite_rpc_core::types::BlockStream;
use solana_sdk::clock::Slot;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError::{Closed, Lagged};
use tokio::sync::broadcast::Sender;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// put everything required to serve sync data calls here
#[derive(Clone)]
pub struct PrioFeeStore {
    // store priofees stats for recently processed blocks up to CLEANUP_SLOTS_AFTER
    recent: Arc<RwLock<BTreeMap<Slot, BlockPrioData>>>,
}

pub struct PrioFeesService {
    pub block_fees_store: PrioFeeStore,
    // use .subscribe() to get a receiver
    pub block_fees_stream: Sender<PrioFeesUpdateMessage>,
}

impl PrioFeesService {
    pub async fn get_latest_priofees(&self) -> Option<(Slot, PrioFeesStats)> {
        let lock = self.block_fees_store.recent.read().await;
        lock.last_key_value()
            .map(|(slot, value)| (*slot, value.calculate_stats()))
    }

    pub async fn get_last_n_priofees_aggregate(&self, nb: usize) -> Option<(Slot, PrioFeesStats)> {
        let lock = self.block_fees_store.recent.read().await;
        let last_slot = match lock.last_key_value().map(|x| *x.0) {
            Some(slot) => slot,
            None => {
                return None;
            }
        };

        let prio_fees = lock
            .iter()
            .rev()
            .take(nb)
            .map(|x| x.1)
            .fold(BlockPrioData::default(), |acc, x| acc.add(x))
            .calculate_stats();
        Some((last_slot, prio_fees))
    }
}

pub fn start_block_priofees_task(
    mut block_stream: BlockStream,
    slots_to_retain: u64,
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

                    let tx_prioritization = block
                        .transactions
                        .iter()
                        .filter(|tx| !tx.is_vote)
                        .map(|tx| PrioFeesData {
                            priority: tx.prioritization_fees.unwrap_or_default(),
                            cu_consumed: tx.cu_consumed.unwrap_or_default(),
                        })
                        .collect::<Vec<PrioFeesData>>();

                    let nb_total_tx = block.transactions.len() as u64;

                    let nb_non_vote_tx =
                        block.transactions.iter().filter(|tx| !tx.is_vote).count() as u64;

                    let total_cu_consumed = block
                        .transactions
                        .iter()
                        .map(|tx| tx.cu_consumed.unwrap_or(0))
                        .sum::<u64>();

                    let non_vote_cu_consumed = block
                        .transactions
                        .iter()
                        .filter(|tx| !tx.is_vote)
                        .map(|tx| tx.cu_consumed.unwrap_or(0))
                        .sum::<u64>();

                    let block_prio_data = BlockPrioData {
                        transaction_data: tx_prioritization,
                        nb_non_vote_tx,
                        nb_total_tx,
                        non_vote_cu_consumed,
                        total_cu_consumed,
                    };

                    trace!("Got prio fees stats for processed block {}", processed_slot);
                    let priofees_stats = block_prio_data.calculate_stats();
                    {
                        // first do some cleanup
                        let mut lock = recent_data.write().await;
                        lock.insert(processed_slot, block_prio_data);
                        lock.retain(|slot, _| {
                            *slot > processed_slot.saturating_sub(slots_to_retain)
                        });
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
