use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use solana_sdk::slot_history::Slot;
use tokio::sync::broadcast::Receiver;

use crate::AtomicSlot;

pub const AVERAGE_SLOT_CHANGE_TIME_IN_MILLIS: u64 = 400;

/// a centralized clock
#[derive(Debug, Clone, Default)]
pub struct SlotClock {
    /// last verified slot from validator
    current_slot: AtomicSlot,
    /// estimated slot in case of log
    estimated_slot: AtomicSlot,
}

impl SlotClock {
    pub fn new(slot: Slot) -> Self {
        Self {
            current_slot: Arc::new(AtomicU64::new(slot)),
            estimated_slot: Arc::new(AtomicU64::new(slot)),
        }
    }

    pub fn get_current_slot(&self) -> u64 {
        self.current_slot.load(Ordering::Relaxed)
    }

    pub fn get_estimated_slot(&self) -> u64 {
        self.estimated_slot.load(Ordering::Relaxed)
    }

    // Estimates the slots, either from polled slot or by forcefully updating after every 400ms
    // returns the estimated slot if current slot is not updated
<<<<<<< HEAD
    pub async fn set_slot(&self, slot_update_notifier: &mut UnboundedReceiver<Slot>) -> Slot {
=======
    pub async fn update_slots_from_subscription(
        &self,
        slot_update_notifier: &mut Receiver<Slot>,
    ) -> u64 {
>>>>>>> 9d7eab2 (Creating a new library for cluster endpoings, Some more refactoring,)
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let estimated_slot = self.estimated_slot.load(Ordering::Relaxed);

        match tokio::time::timeout(
            Duration::from_millis(AVERAGE_SLOT_CHANGE_TIME_IN_MILLIS),
            slot_update_notifier.recv(),
        )
        .await
        {
            Ok(Ok(slot)) => {
                // slot is latest
                if slot > current_slot {
                    self.current_slot.store(slot, Ordering::Relaxed);
                    if current_slot > estimated_slot {
                        self.estimated_slot.store(slot, Ordering::Relaxed);
                    }
                }
            }
            Ok(Err(_)) => log::error!("got nothing from slot update notifier"),
            Err(err) => {
                log::warn!("failed to receive slot update: {err}");
                // force update the slot
                // estimated slot should not go ahead more than 32 slots
                // this is because it may be a slot block
                if estimated_slot < current_slot + 32 {
                    self.estimated_slot.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        self.estimated_slot.load(Ordering::Relaxed)
    }
}
