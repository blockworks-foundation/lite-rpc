use std::collections::HashMap;
use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::Arc;
use async_trait::async_trait;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::{broadcast::Receiver, broadcast::Sender};
use solana_lite_rpc_core::quic_connection_utils::QuicConnectionParameters;
use solana_lite_rpc_core::structures::identity_stakes::IdentityStakes;
use solana_lite_rpc_core::tx_store::TxStore;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TpuConnectionPath {
    QuicDirectPath,
    QuicForwardProxyPath { forward_proxy_address: SocketAddr },
}

impl Display for TpuConnectionPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TpuConnectionPath::QuicDirectPath => write!(f, "Direct QUIC connection to TPU"),
            TpuConnectionPath::QuicForwardProxyPath { forward_proxy_address } => {
                write!(f, "QUIC Forward Proxy on {}", forward_proxy_address)
            }
        }
    }
}
