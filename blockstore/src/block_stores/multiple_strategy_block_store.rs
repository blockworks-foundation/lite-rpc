use crate::block_stores::faithful_history::faithful_block_store::FaithfulBlockStore;
use crate::block_stores::postgres::postgres_block_store_query::PostgresQueryBlockStore;
use anyhow::{bail, Context, Result};
use log::{debug, trace};
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::slot_history::Slot;
use std::ops::{Deref, RangeInclusive};
use std::sync::Arc;
use jsonrpsee::tracing::trace_span;

#[derive(Debug, Clone)]
pub enum BlockSource {
    // serve two epochs from postgres
    RecentEpochDatabase,
    // serve epochs older than two from faithful_history service
    FaithfulArchive,
}

#[derive(Debug, Clone)]
pub struct BlockStorageData {
    // note: commitment_config is the actual commitment level
    pub block: ProducedBlock,
    // meta data
    pub result_source: BlockSource,
}

impl Deref for BlockStorageData {
    type Target = ProducedBlock;
    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

// you might need to add a read-cache instead
pub struct MultipleStrategyBlockStorage {
    block_storage_query: PostgresQueryBlockStore,
    // note supported ATM
    faithful_block_storage: Option<FaithfulBlockStore>, // to fetch legacy blocks from faithful_history
                                                        // last_confirmed_slot: Arc<AtomicU64>,
}

impl MultipleStrategyBlockStorage {
    pub fn new(
        block_storage_query: PostgresQueryBlockStore,
        _faithful_rpc_client: Option<Arc<RpcClient>>,
    ) -> Self {
        Self {
            block_storage_query,
            // faithful_history not used ATM
            faithful_block_storage: None,
            // faithful_block_storage: faithful_rpc_client.map(|rpc| FaithfulBlockStore::new(rpc)),
        }
    }

    // we need to build the slots from right to left
    pub async fn get_slot_range(&self) -> RangeInclusive<Slot> {
        // merge them
        let persistent_storage_range = self.block_storage_query.get_slot_range().await;
        trace!("Persistent storage range: {:?}", persistent_storage_range);

        let mut lower = *persistent_storage_range.start();

        if let Some(faithful_block_storage) = &self.faithful_block_storage {
            let faithful_storage_range = faithful_block_storage.get_slot_range();
            trace!("Faithful storage range: {:?}", faithful_storage_range);
            if lower.saturating_sub(*faithful_storage_range.end()) <= 1 {
                // move the lower bound to the left
                lower = lower.min(*faithful_storage_range.start());
            }
        }

        let merged = RangeInclusive::new(lower, *persistent_storage_range.end());
        trace!(
            "Merged range from database + faithful_history: {:?}",
            merged
        );

        merged
    }

    // lookup confirmed or finalized block from either our blockstore or faithful_history
    // TODO find better method name
    pub async fn query_block(
        &self,
        slot: solana_sdk::slot_history::Slot,
    ) -> Result<BlockStorageData> {
        // TODO this check is optional and might be moved to the caller
        // if slot > last_confirmed_slot {
        //     bail!(format!(
        //         "Block {} not found (last_confirmed_slot={})",
        //         slot, last_confirmed_slot
        //     ));
        // }

        // TODO: use a smarter strategy to decide about the cutoff
        // current strategy:
        // 1. check if requested slot is in min-max range served from Postgres
        // 2.1. if yes; fetch from Postgres
        // 2.2. if not: try to fetch from faithful_history

        match self.block_storage_query.is_block_in_range(slot).await {
            true => {
                debug!(
                    "Assume block {} to be available in persistent block-storage",
                    slot,
                );
                let lookup = self
                    .block_storage_query
                    .query_block(slot)
                    .await
                    .context(format!("block {} not found although it was in range", slot));

                return lookup.map(|b| BlockStorageData {
                    block: b,
                    result_source: BlockSource::RecentEpochDatabase,
                });
            }
            false => {
                debug!(
                    "Block {} not found in persistent block-storage - continue",
                    slot
                );
            }
        }

        if let Some(faithful_block_storage) = &self.faithful_block_storage {
            match faithful_block_storage.get_block(slot).await {
                Ok(block) => {
                    debug!(
                        "Lookup for block {} successful in faithful_history block-storage",
                        slot
                    );

                    Ok(BlockStorageData {
                        block,
                        result_source: BlockSource::FaithfulArchive,
                    })
                }
                Err(_) => {
                    debug!(
                        "Block {} not found in faithful_history storage - giving up",
                        slot
                    );
                    bail!(format!("Block {} not found in faithful_history", slot));
                }
            }
        } else {
            bail!(format!(
                "Block {} not found - faithful_history not available",
                slot
            ));
        }
    }
}
