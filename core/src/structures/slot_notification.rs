use solana_sdk::slot_history::Slot;

#[derive(Debug, Clone, Default)]
pub struct SlotNotification {
    pub processed_slot: Slot,
    pub estimated_processed_slot: Slot
}