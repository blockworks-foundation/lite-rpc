use solana_streamer::nonblocking::quic::ConnectionPeerType;

#[derive(Debug, Copy, Clone)]
pub struct IdentityStakes {
    pub peer_type: ConnectionPeerType,
    pub stakes: u64,
    pub total_stakes: u64,
    pub min_stakes: u64,
    pub max_stakes: u64,
}

impl Default for IdentityStakes {
    fn default() -> Self {
        IdentityStakes {
            peer_type: ConnectionPeerType::Unstaked,
            stakes: 0,
            total_stakes: 0,
            max_stakes: 0,
            min_stakes: 0,
        }
    }
}
