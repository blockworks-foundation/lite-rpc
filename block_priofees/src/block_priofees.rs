use crate::rpc_data::{PrioFeesStats, PrioFeesUpdateMessage};
use crate::stats_calculation::calculate_supp_stats;
use dashmap::DashMap;
use log::{error, info, trace, warn};
use solana_lite_rpc_core::types::BlockStream;
use solana_sdk::clock::Slot;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError::{Closed, Lagged};
use tokio::sync::broadcast::Sender;
use tokio::task::JoinHandle;

// note: ATM only the latest slot (highest key) is used
// note: if that value grows we should move from DashMap to a more efficient data structure (TreeMap)
const SLOTS_TO_RETAIN: u64 = 100;

/// put everything required to serve sync data calls here
#[derive(Clone)]
pub struct PrioFeeStore {
    // store priofees stats for recently confirmed blocks up to CLEANUP_SLOTS_AFTER
    recent: Arc<DashMap<Slot, PrioFeesStats>>,
}

pub struct PrioFeesService {
    pub block_fees_store: PrioFeeStore,
    // use .subscribe() to get a receiver
    pub block_fees_stream: Sender<PrioFeesUpdateMessage>,
}

impl PrioFeesService {
    pub async fn get_latest_priofees(&self) -> Option<(Slot, PrioFeesStats)> {
        let latest_in_store = self
            .block_fees_store
            .recent
            .iter()
            .max_by(|a, b| a.key().cmp(b.key()));
        return latest_in_store.map(|x| (x.key().clone(), x.value().clone()));
    }
}

pub async fn start_block_priofees_task(
    mut block_stream: BlockStream,
) -> (JoinHandle<()>, PrioFeesService) {
    let recent_data = Arc::new(DashMap::new());
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
                    // first do some cleanup
                    recent_data.retain(|slot, _| *slot > slot - SLOTS_TO_RETAIN);

                    if !block.commitment_config.is_confirmed() {
                        continue;
                    }
                    let confirmed_slot = block.slot;

                    let block_priofees = block
                        .transactions
                        .iter()
                        .map(|tx| {
                            (
                                tx.prioritization_fees.unwrap_or_default(),
                                tx.cu_consumed.unwrap_or_default(),
                            )
                        })
                        .collect::<Vec<(u64, u64)>>();

                    let priofees_stats = calculate_supp_stats(&block_priofees);

                    trace!("Got prio fees stats for confirmed block {}", confirmed_slot);

                    recent_data.insert(confirmed_slot, priofees_stats.clone());
                    let msg = PrioFeesUpdateMessage {
                        slot: confirmed_slot,
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
