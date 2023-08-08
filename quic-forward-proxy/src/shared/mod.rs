use solana_sdk::transaction::VersionedTransaction;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

/// internal structure with transactions and target TPU
#[derive(Debug)]
pub struct ForwardPacket {
    pub transactions: Vec<VersionedTransaction>,
    pub tpu_address: SocketAddr,
}

impl ForwardPacket {
    pub fn shard_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        // note: assumes that there are transactions with >=0 signatures
        self.transactions[0].signatures[0].hash(&mut hasher);
        hasher.finish()
    }
}
