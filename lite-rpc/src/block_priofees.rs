use std::ops::Deref;
use std::sync::Arc;
use dashmap::DashMap;
use dashmap::mapref::multiple::RefMulti;
use jsonrpsee::core::Serialize;
use jsonrpsee::tracing::field::debug;
use log::{debug, error, info, trace, warn};
use solana_rpc_client_api::response::Fees;
use solana_sdk::clock::Slot;
use tokio::sync::broadcast::Sender;
use tokio::sync::broadcast::error::RecvError::{Closed, Lagged};
use tokio::sync::broadcast::error::SendError;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use solana_lite_rpc_cluster_endpoints::CommitmentLevel;
use solana_lite_rpc_core::stores::data_cache::{DataCache, SlotCache};
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::types::BlockStream;

/// put everything required to serve sync data calls here
#[derive(Clone)]
pub struct PrioFeeStore {
    // TODO cleanup
    recent: Arc<DashMap<Slot, PrioFeesStats>>,
    slot_cache: SlotCache,
}

pub struct PrioFeesService {
    pub block_fees_store: PrioFeeStore,
    // use .subscribe() to get a receiver
    pub block_fees_stream: Sender<PrioFeesUpdateMessage>,
}

impl PrioFeesService {
    pub async fn get_latest_priofees(&self) -> Option<(Slot, PrioFeesStats)> {
        let latest_in_store = self.block_fees_store.recent.iter().max_by(|a, b| a.key().cmp(b.key()));
        return latest_in_store.map(|x| (x.key().clone(), x.value().clone()));
    }
}


pub async fn start_priofees_task(data_cache: DataCache, mut block_stream: BlockStream) -> (JoinHandle<()>, PrioFeesService) {
    let recent_data = Arc::new(DashMap::new());
    let store = PrioFeeStore {
        recent: recent_data.clone(),
        slot_cache: data_cache.slot_cache,
    };
    let (priofees_update_sender, _priofees_update_receiver) = tokio::sync::broadcast::channel(1); // TODO set to hiher value
    let sender_to_return = priofees_update_sender.clone();

    let jh_priofees_task = tokio::spawn(async move {
        let sender = priofees_update_sender.clone();
        'recv_loop: loop {
            let block = block_stream.recv().await;
            match block {
                Ok(block) => {
                    if !block.commitment_config.is_confirmed() {
                        continue;
                    }
                    let slot = block.slot;

                    // first do some cleanup
                    recent_data.retain(|slot, _| *slot > slot - 100);

                    let block_prio_fees =
                        block.transactions.iter().map(|tx| {
                            (tx.prioritization_fees.unwrap_or_default(), tx.cu_consumed.unwrap_or_default())
                        }).collect::<Vec<(u64, u64)>>();

                    let priofees_stats = calculate_supp_info(&block_prio_fees);

                    trace!("Got prio fees stats for block {}", slot);

                    recent_data.insert(slot, priofees_stats.clone());
                    let msg = PrioFeesUpdateMessage {
                        slot,
                        priofees_stats,
                    };
                    let send_result = sender.send(msg);
                    match send_result {
                        Ok(n_subscribers) => {
                            trace!("sent priofees update message to {} subscribers (buffer={})", n_subscribers, sender.len());
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
        }
    )
}

#[derive(Clone, Debug)]
pub struct PrioFeesUpdateMessage {
    pub slot: Slot,
    pub priofees_stats: PrioFeesStats,
}

// used as RPC DTO
#[derive(Clone, Serialize, Debug)]
pub struct PrioFeesStats {
    pub p_min: u64,
    pub p_median: u64,
    pub p_75: u64,
    pub p_90: u64,
    pub p_max: u64,
}


fn calculate_supp_info(
    // Vec(prioritization_fees, cu_consumed)
    prio_fees_in_block: &Vec<(u64, u64)>,
) -> PrioFeesStats {
    let mut prio_fees_in_block = if prio_fees_in_block.is_empty() {
        // TODO is that smart?
        vec![(0, 0)]
    } else {
        prio_fees_in_block.clone()
    };
    prio_fees_in_block.sort_by(|a, b| a.0.cmp(&b.0));

    let median_index = prio_fees_in_block.len() / 2;
    let p75_index = prio_fees_in_block.len() * 75 / 100;
    let p90_index = prio_fees_in_block.len() * 90 / 100;
    let p_min = prio_fees_in_block[0].0;
    let p_median = prio_fees_in_block[median_index].0;
    let p_75 = prio_fees_in_block[p75_index].0;
    let p_90 = prio_fees_in_block[p90_index].0;
    let p_max = prio_fees_in_block.last().map(|x| x.0).unwrap();

    PrioFeesStats {
        p_min,
        p_median,
        p_75,
        p_90,
        p_max,
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_supp_info() {
        let prio_fees_in_block = vec![
            (2, 2),
            (4, 4),
            (5, 5),
            (3, 3),
            (1, 1),
        ];
        let supp_info = calculate_supp_info(&prio_fees_in_block);
        assert_eq!(supp_info.p_min, 1);
        assert_eq!(supp_info.p_median, 3);
        assert_eq!(supp_info.p_75, 4);
        assert_eq!(supp_info.p_90, 5);
        assert_eq!(supp_info.p_max, 5);
    }
}
