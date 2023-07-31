use solana_sdk::transaction::VersionedTransaction;
use std::net::SocketAddr;

/// internal structure with transactions and target TPU
#[derive(Debug)]
pub struct ForwardPacket {
    pub transactions: Vec<VersionedTransaction>,
    pub tpu_address: SocketAddr,
}
