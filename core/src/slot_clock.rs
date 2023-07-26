use crate::AtomicSlot;

/// a centralized clock
#[derive(Debug, Clone, Default)]
pub struct SlotClock {
    /// last verified slot from validator
    current_slot: AtomicSlot,
    /// estimated slot in case of log
    estimated_slot: AtomicSlot,
}

impl SlotClock {
    // Estimates the slots, either from polled slot or by forcefully updating after every 400ms
    // returns if the estimated slot was updated or not
    pub async fn set_slot(&self, slot_update_notifier: &mut UnboundedReceiver<u64>) {
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let estimated_slot = self.estimated_slot.load(Ordering::Relaxed);

        match tokio::time::timeout(
            Duration::from_millis(AVERAGE_SLOT_CHANGE_TIME_IN_MILLIS),
            slot_update_notifier.recv(),
        )
        .await
        {
            Ok(Some(slot)) => {
                // slot is latest
                if slot > current_slot {
                    self.current_slot.store(slot, Ordering::Relaxed);
                    if current_slot > estimated_slot {
                        self.estimated_slot.store(slot, Ordering::Relaxed);
                    }
                }
            }
            Ok(None) => (),
            Err(_) => {
                // force update the slot
                // estimated slot should not go ahead more than 32 slots
                // this is because it may be a slot block
                if estimated_slot < current_slot + 32 {
                    estimated_slot += 1;
                }
            }
        }

        (current_slot, estimated_slot)
    }
}
