use solana_sdk::signature::Signature;
use solana_sdk::slot_history::Slot;

pub type WireTransaction = Vec<u8>;

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct SentTransactionInfo {
    pub signature: Signature,
    pub slot: Slot,
    pub transaction: WireTransaction,
    pub last_valid_block_height: u64,
}
