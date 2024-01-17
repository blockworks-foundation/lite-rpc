use std::sync::{atomic::AtomicU64, Arc};
use solana_lite_rpc_core::structures::slot_notification::SlotNotification;
use solana_sdk::slot_history::Slot;

#[derive(Default, Clone)]
pub struct SlotCache {
    current_slot: Arc<AtomicU64>,
    estimated_slot: Arc<AtomicU64>,
}

impl SlotCache {
    pub fn new(slot: Slot) -> Self {
        Self {
            current_slot: Arc::new(AtomicU64::new(slot)),
            estimated_slot: Arc::new(AtomicU64::new(slot)),
        }
    }
    pub fn get_current_slot(&self) -> Slot {
        self.current_slot.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn get_estimated_slot(&self) -> Slot {
        self.estimated_slot
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn update(&self, slot_notification: SlotNotification) {
        self.current_slot.store(
            slot_notification.processed_slot,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.estimated_slot.store(
            slot_notification.estimated_processed_slot,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}
