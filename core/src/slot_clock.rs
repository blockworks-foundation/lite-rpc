use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::broadcast;

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
    pub fn get_current_slot(&self) -> u64 {
        self.current_slot.load(Ordering::Relaxed)
    }

    pub fn get_estimated_slot(&self) -> u64 {
        self.estimated_slot.load(Ordering::Relaxed)
    }

    // Estimates the slots, either from polled slot or by forcefully updating after every 400ms
    // returns if the estimated slot was updated or not
    pub async fn set_slot(&self, slot_update_notifier: &mut broadcast::Receiver<u64>) -> bool {
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
                        return true;
                    }
                }
            }
            Ok(Err(err)) => log::error!("failed to receive slot update: {:?}", err),
            Err(_) => {
                // force update the slot
                // estimated slot should not go ahead more than 32 slots
                // this is because it may be a slot block
                if estimated_slot < current_slot + 32 {
                    self.estimated_slot
                        .store(estimated_slot + 1, Ordering::Relaxed);
                }
            }
        }

        false
    }
}
