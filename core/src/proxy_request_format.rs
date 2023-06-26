use std::net::{SocketAddr, SocketAddrV4};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TpuForwardingRequestV1 {
    pub tpu_socket_addr: SocketAddr, // TODO is that correct
    pub identity_tpunode: Pubkey,
    pub transactions: Vec<VersionedTransaction>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TpuForwardingRequest {
    V1(TpuForwardingRequestV1),
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
        }
    }

    pub fn get_identity_tpunode(&self) -> Pubkey {
        match self {
            TpuForwardingRequest::V1(request) => request.identity_tpunode,
        }
    }

    pub fn get_transactions(&self) -> Vec<VersionedTransaction> {
        match self {
            TpuForwardingRequest::V1(request) => request.transactions.clone(),
        }
    }
}

fn serialize_tpu_forwarding_request(
    tpu_socket_addr: SocketAddr,
    tpu_identity: Pubkey,
    transactions: Vec<VersionedTransaction>) -> Vec<u8> {

    let request = TpuForwardingRequest::new(tpu_socket_addr, tpu_identity, transactions);

    bincode::serialize(&request).expect("Expect to serialize transactions")
}

// TODO reame
fn deserialize_tpu_forwarding_request(raw_proxy_request: &Vec<u8>) -> TpuForwardingRequest {
    let request = bincode::deserialize::<TpuForwardingRequest>(&raw_proxy_request)
        .context("deserialize proxy request")
        .unwrap();

    request
}

