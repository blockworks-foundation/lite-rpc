use solana_sdk::transaction::VersionedTransaction;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

/// internal structure with transactions and target TPU
#[derive(Debug)]
pub struct ForwardPacket {
    pub transactions: Vec<VersionedTransaction>,
    pub tpu_address: SocketAddr,
    pub shard_hash: u64,
}

impl ForwardPacket {

    pub fn new(transactions: &[VersionedTransaction], tpu_address: SocketAddr) -> Self {
        assert!(!transactions.is_empty(), "no transactions");
        let hash = Self::shard_hash(&transactions);
        Self {
            transactions: transactions.to_vec(),
            tpu_address,
            shard_hash: hash,
        }
    }

    fn shard_hash(transactions: &[VersionedTransaction]) -> u64 {
        let mut hasher = DefaultHasher::new();
        // note: assumes that there are transactions with >=0 signatures
        transactions[0].signatures[0].hash(&mut hasher);
        hasher.finish()
    }
}
