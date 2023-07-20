use std::fmt::Display;
use std::net::SocketAddr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TpuConnectionPath {
    QuicDirect,
    QuicForwardProxy { forward_proxy_address: SocketAddr },
}

impl Display for TpuConnectionPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TpuConnectionPath::QuicDirect => write!(f, "Direct QUIC connection to TPU"),
            TpuConnectionPath::QuicForwardProxy { forward_proxy_address } => {
                write!(f, "QUIC Forward Proxy on {}", forward_proxy_address)
            }
        }
    }
}
