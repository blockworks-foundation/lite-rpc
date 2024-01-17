use solana_sdk::{pubkey::Pubkey, slot_history::Slot};

#[derive(Debug, Clone)]
pub struct LeaderData {
    pub leader_slot: Slot,
    pub pubkey: Pubkey,
}
