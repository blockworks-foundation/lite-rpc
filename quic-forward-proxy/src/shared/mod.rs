use std::net::SocketAddr;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;


#[derive(Debug)]
pub struct TxRawData {
    pub signature: String,
    pub transaction: Vec<u8>,
}

/// internal structure with transactions and target TPU
#[derive(Debug)]
pub struct ForwardPacket {
    pub transactions: Vec<Vec<u8>>,
    pub tpu_address: SocketAddr,
    pub tpu_identity: Pubkey,
    pub shard_hash: u64,
}

impl ForwardPacket {
    pub fn new(transactions: Vec<Vec<u8>>, tpu_address: SocketAddr,
               tpu_identity: Pubkey,
               hash: u64) -> Self {
        assert!(!transactions.is_empty(), "no transactions");
        Self {
            transactions,
            tpu_address,
            tpu_identity,
            shard_hash: hash,
        }
    }
}
