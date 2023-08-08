use anyhow::Context;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use std::fmt;
use std::fmt::Display;
use std::net::SocketAddr;

///
/// lite-rpc to proxy wire format
/// compat info: non-public format ATM
/// initial version
const FORMAT_VERSION1: u16 = 2400;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TxData(Signature, Vec<u8>);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TpuForwardingRequest {
    format_version: u16,
    tpu_socket_addr: SocketAddr,
    identity_tpunode: Pubkey, // note: this is only used for debugging
    transactions: Vec<TxData>,
}

impl Display for TpuForwardingRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TpuForwardingRequest for tpu target {} with identity {}",
            &self.get_tpu_socket_addr(),
            &self.get_identity_tpunode(),
        )
    }
}

impl TpuForwardingRequest {
    pub fn new(
        tpu_socket_addr: SocketAddr,
        identity_tpunode: Pubkey,
        transactions: Vec<VersionedTransaction>,
    ) -> Self {
        TpuForwardingRequest {
            format_version: FORMAT_VERSION1,
            tpu_socket_addr,
            identity_tpunode,
            transactions: transactions.iter().map(Self::serialize).collect_vec(),
        }
    }

    fn serialize(tx: &VersionedTransaction) -> TxData {
        TxData(tx.signatures[0], bincode::serialize(&tx).unwrap())
    }

    pub fn serialize_wire_format(&self) -> Vec<u8> {
        bincode::serialize(&self).expect("Expect to serialize transactions")
    }

    pub fn deserialize_from_raw_request(raw_proxy_request: &[u8]) -> TpuForwardingRequest {
        let request = bincode::deserialize::<TpuForwardingRequest>(raw_proxy_request)
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
}
