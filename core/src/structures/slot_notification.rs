use std::sync::{atomic::AtomicU64, Arc};

use solana_sdk::slot_history::Slot;

pub type AtomicSlot = Arc<AtomicU64>;

#[derive(Debug, Clone, Default)]
pub struct SlotNotification {
    pub processed_slot: Slot,
    pub estimated_processed_slot: Slot,
}
