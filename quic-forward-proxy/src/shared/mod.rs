use std::net::SocketAddr;

/// internal structure with transactions and target TPU
#[derive(Debug)]
pub struct ForwardPacket {
    pub transactions: Vec<Vec<u8>>,
    pub tpu_address: SocketAddr,
    pub shard_hash: u64,
}

impl ForwardPacket {
    pub fn new(transactions: Vec<Vec<u8>>, tpu_address: SocketAddr, hash: u64) -> Self {
        assert!(!transactions.is_empty(), "no transactions");
        Self {
            transactions,
            tpu_address,
            shard_hash: hash,
        }
    }
}
