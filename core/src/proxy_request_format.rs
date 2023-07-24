use std::fmt;
use std::fmt::Display;
use std::net::{SocketAddr, SocketAddrV4};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;

///
/// lite-rpc to proxy wire format
/// compat info: non-public format ATM
/// initial version
const FORMAT_VERSION1: u16 = 2301;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TpuForwardingRequest {
    format_version: u16,
    tpu_socket_addr: SocketAddr, // TODO is that correct
    identity_tpunode: Pubkey,
    transactions: Vec<VersionedTransaction>,
}

impl Display for TpuForwardingRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TpuForwardingRequest for tpu target {} with identity {}: payload {} tx",
               &self.get_tpu_socket_addr(), &self.get_identity_tpunode(), &self.get_transactions().len())
    }
}

impl TpuForwardingRequest {
    pub fn new(tpu_socket_addr: SocketAddr, identity_tpunode: Pubkey,
               transactions: Vec<VersionedTransaction>) -> Self {
        TpuForwardingRequest {
            format_version: FORMAT_VERSION1,
            tpu_socket_addr,
            identity_tpunode,
            transactions,
        }
    }

    pub fn serialize_wire_format(
        &self) -> Vec<u8> {
        bincode::serialize(&self).expect("Expect to serialize transactions")
    }

    // TODO reame
    pub fn deserialize_from_raw_request(raw_proxy_request: &Vec<u8>) -> TpuForwardingRequest {
        let request = bincode::deserialize::<TpuForwardingRequest>(&raw_proxy_request)
            .context("deserialize proxy request")
            .unwrap();

        assert_eq!(request.format_version, 2301);

        request
    }

    pub fn get_tpu_socket_addr(&self) -> SocketAddr {
        self.tpu_socket_addr
    }

    pub fn get_identity_tpunode(&self) -> Pubkey {
        self.identity_tpunode
    }

    pub fn get_transactions(&self) -> Vec<VersionedTransaction> {
        self.transactions.clone()
    }
}



