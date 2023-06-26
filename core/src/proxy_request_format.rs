use std::fmt;
use std::fmt::Display;
use std::net::{SocketAddr, SocketAddrV4};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;

// TODO define a proper discriminator
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TpuForwardingRequest {
    V1(TpuForwardingRequestV1),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TpuForwardingRequestV1 {
    tpu_socket_addr: SocketAddr, // TODO is that correct
    identity_tpunode: Pubkey,
    transactions: Vec<VersionedTransaction>,
}

impl Display for TpuForwardingRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TpuForwardingRequest for tpu target {} with indentity {}",
               &self.get_tpu_socket_addr(), &self.get_identity_tpunode())
    }
}

impl TpuForwardingRequest {
    pub fn new(tpu_socket_addr: SocketAddr, identity_tpunode: Pubkey,
               transactions: Vec<VersionedTransaction>) -> Self {
        TpuForwardingRequest::V1(
            TpuForwardingRequestV1 {
                tpu_socket_addr,
                identity_tpunode,
                transactions,
            })
    }

    pub fn get_tpu_socket_addr(&self) -> SocketAddr {
        match self {
            TpuForwardingRequest::V1(request) => request.tpu_socket_addr,
            _ => panic!("format version error"),
        }
    }

    pub fn get_identity_tpunode(&self) -> Pubkey {
        match self {
            TpuForwardingRequest::V1(request) => request.identity_tpunode,
            _ => panic!("format version error"),
        }
    }

    pub fn get_transactions(&self) -> Vec<VersionedTransaction> {
        match self {
            TpuForwardingRequest::V1(request) => request.transactions.clone(),
            _ => panic!("format version error"),
        }
    }
}



