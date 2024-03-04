use crate::stores::block_information_store::BlockInformation;
use crate::stores::data_cache::DataCache;
use serde::Serialize;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::{Hash, ParseHashError};
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

pub async fn get_current_confirmed_slot(data_cache: &DataCache) -> u64 {
    let commitment = CommitmentConfig::confirmed();
    let BlockInformation { slot, .. } = data_cache
        .block_information_store
        .get_latest_block(commitment)
        .await;
    slot
}

// shameless copy from Hash::from_str which is declared private
const MAX_BASE58_LEN: usize = 44;
pub fn hash_from_str(s: &str) -> Result<Hash, ParseHashError> {
    if s.len() > MAX_BASE58_LEN {
        return Err(ParseHashError::WrongSize);
    }
    let bytes = bs58::decode(s)
        .into_vec()
        .map_err(|_| ParseHashError::Invalid)?;
    if bytes.len() != std::mem::size_of::<Hash>() {
        Err(ParseHashError::WrongSize)
    } else {
        Ok(Hash::new(&bytes))
    }
}
