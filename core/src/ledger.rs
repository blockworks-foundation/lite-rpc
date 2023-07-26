use crate::{block_store::BlockStore, slot_clock::SlotClock, tx_store::TxStore};

/// The central data store for all data from the cluster.
#[derive(Default)]
pub struct Ledger {
    blocks: BlockStore,
    clock: SlotClock,
    txs: TxStore,
}
