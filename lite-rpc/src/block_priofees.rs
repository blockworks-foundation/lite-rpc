use std::sync::Arc;
use dashmap::DashMap;
use log::{error, info};
use solana_rpc_client_api::response::Fees;
use solana_sdk::clock::Slot;
use tokio::sync::broadcast::Receiver;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::types::BlockStream;

pub struct BlockPrioFees {
    // TODO add more
    median: u64,
}

#[derive(Clone)]
pub struct PrioFeeStore {
    // TODO cleanup
    recent: Arc<DashMap<Slot, BlockPrioFees>>
}

pub struct PrioFeesService {
    pub block_fees_store: PrioFeeStore,
}

impl PrioFeesService {
    pub fn new() -> Self {
        let block_fees_store = PrioFeeStore {
            recent: Arc::new(DashMap::new()),
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
                for tx in block.transactions {
                    info!("tx: {:?} - prio fees {:?}", tx.signature, tx.prioritization_fees);
                    tx.prioritization_fees;
                }

                let fees = BlockPrioFees {
                    median: 9999,
                };

                store.recent.insert(block.slot, fees);

            }
            Err(e) => {
                error!("failed to receive block: {:?}", e);
                break;
            }
        }
    }
}
