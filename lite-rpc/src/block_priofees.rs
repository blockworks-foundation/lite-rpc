use std::ops::Deref;
use std::sync::Arc;
use dashmap::DashMap;
use dashmap::mapref::multiple::RefMulti;
use jsonrpsee::core::Serialize;
use jsonrpsee::tracing::field::debug;
use log::{debug, error, info};
use solana_rpc_client_api::response::Fees;
use solana_sdk::clock::Slot;
use tokio::sync::broadcast::Receiver;
use solana_lite_rpc_cluster_endpoints::CommitmentLevel;
use solana_lite_rpc_core::stores::data_cache::{DataCache, SlotCache};
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::types::BlockStream;

#[derive(Clone)]
pub struct PrioFeeStore {
    // TODO cleanup
    recent: Arc<DashMap<Slot, PrioritizationFeesInfo>>,
    slot_cache: SlotCache,
}

pub struct PrioFeesService {
    pub block_fees_store: PrioFeeStore,
}

impl PrioFeesService {
    pub async fn get_median_priofees(&self) -> Option<PrioritizationFeesInfo> {
        // TODO remove
        let slot_slotcache = self.block_fees_store.slot_cache.get_current_slot();
        info!("current slot (according to slot cache) is {}", slot_slotcache);
        let highest_slotnumber = self.block_fees_store.recent.iter().max_by(|a, b| a.key().cmp(b.key()));

        return highest_slotnumber.map(|x| x.value().clone());

        // self.block_fees_store.recent.
        // let lookup = self.block_fees_store.recent.get(&slot);
        // return (slot, foo);
    }
}

impl PrioFeesService {
    pub fn new(data_cache: DataCache) -> Self {
        let block_fees_store = PrioFeeStore {
            recent: Arc::new(DashMap::new()),
            slot_cache: data_cache.slot_cache,
        };
        Self {
            block_fees_store
        }
    }

}

pub async fn start_priofees_service(store: PrioFeeStore, mut block_stream: BlockStream) {
    loop {
        let block = block_stream.recv().await;
        match block {
            Ok(block) => {
                if !block.commitment_config.is_confirmed() {
                    continue;
                }
                let slot = block.slot;

                // first do some cleanup
                store.recent.retain(|slot, _| *slot > slot - 100);

                let block_prio_fees =
                    block.transactions.iter().map(|tx| {
                        (tx.prioritization_fees.unwrap_or_default(), tx.cu_consumed.unwrap_or_default())
                    }).collect::<Vec<(u64, u64)>>();

                let prioritization_fees_info = calculate_supp_info(&block_prio_fees);

                debug!("Store prioritization_fees_info for block {}", slot);
                store.recent.insert(slot, prioritization_fees_info);

            }
            Err(e) => {
                error!("failed to receive block: {:?}", e);
                break;
            }
        }
    }
}


// used as RPC DTO
#[derive(Clone, Serialize, Debug)]
pub struct PrioritizationFeesInfo {
    pub p_min: u64,
    pub p_median: u64,
    pub p_75: u64,
    pub p_90: u64,
    pub p_max: u64,

    pub med_cu: Option<u64>,
    pub p75_cu: Option<u64>,
    pub p90_cu: Option<u64>,
    pub p95_cu: Option<u64>,
}


fn calculate_supp_info(
    // Vec(prioritization_fees, cu_consumed)
    prio_fees_in_block: &Vec<(u64, u64)>,
) -> PrioritizationFeesInfo {
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

    // TODO try to get rid of Option
    let mut med_cu = None;
    let mut p75_cu = None;
    let mut p90_cu = None;
    let mut p95_cu = None;

    // get stats by CU
    let cu_sum: u64 = prio_fees_in_block.iter().map(|x| x.1).sum();
    let mut agg: u64 = 0;
    for (prio, cu) in prio_fees_in_block {
        agg = agg + cu;
        if med_cu.is_none() && agg > (cu_sum as f64 * 0.5) as u64 {
            med_cu = Some(prio);
        } else if p75_cu.is_none() && agg > (cu_sum as f64 * 0.75) as u64 {
            p75_cu = Some(prio)
        } else if p90_cu.is_none() && agg > (cu_sum as f64 * 0.9) as u64 {
            p90_cu = Some(prio);
        } else if p95_cu.is_none() && agg > (cu_sum as f64 * 0.95) as u64 {
            p95_cu = Some(prio)
        }
    }

    PrioritizationFeesInfo {
        p_min,
        p_median,
        p_75,
        p_90,
        p_max,
        med_cu,
        p75_cu,
        p90_cu,
        p95_cu,
    }
}
