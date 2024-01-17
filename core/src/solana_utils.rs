use serde::Serialize;
use solana_sdk::hash::Hash;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::{uses_durable_nonce, Transaction, VersionedTransaction};

/// Trait used to add support for versioned transactions to RPC APIs while
/// retaining backwards compatibility
pub trait SerializableTransaction: Serialize {
    fn get_signature(&self) -> &Signature;
    fn get_recent_blockhash(&self) -> &Hash;
    fn uses_durable_nonce(&self) -> bool;
}
impl SerializableTransaction for Transaction {
    fn get_signature(&self) -> &Signature {
        &self.signatures[0]
    }
    fn get_recent_blockhash(&self) -> &Hash {
        &self.message.recent_blockhash
    }
    fn uses_durable_nonce(&self) -> bool {
        uses_durable_nonce(self).is_some()
    }
}
impl SerializableTransaction for VersionedTransaction {
    fn get_signature(&self) -> &Signature {
        &self.signatures[0]
    }
    fn get_recent_blockhash(&self) -> &Hash {
        self.message.recent_blockhash()
    }
    fn uses_durable_nonce(&self) -> bool {
        self.uses_durable_nonce()
    }
}
