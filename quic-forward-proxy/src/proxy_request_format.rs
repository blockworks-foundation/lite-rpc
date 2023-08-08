use anyhow::Context;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;
use std::fmt;
use std::fmt::Display;
use std::net::SocketAddr;

///
/// lite-rpc to proxy wire format
/// compat info: non-public format ATM
/// initial version
pub const FORMAT_VERSION1: u16 = 2302;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TpuForwardingRequest {
    format_version: u16,
    tpu_socket_addr: SocketAddr,
    identity_tpunode: Pubkey,    // note: this is only used for debugging
    // TODO consider not deserializing transactions in proxy
    transactions: Vec<VersionedTransaction>,
}

impl Display for TpuForwardingRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TpuForwardingRequest for tpu target {} with identity {}: payload {} tx",
            &self.get_tpu_socket_addr(),
            &self.get_identity_tpunode(),
            &self.get_transactions().len()
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
            transactions,
        }
    }

    pub fn try_serialize_wire_format(&self) -> anyhow::Result<Vec<u8>> {
        bincode::serialize(&self)
            .context("serialize proxy request")
            .map_err(anyhow::Error::from)
    }

    pub fn try_deserialize_from_wire_format(
        raw_proxy_request: &[u8],
    ) -> anyhow::Result<TpuForwardingRequest> {
        let request = bincode::deserialize::<TpuForwardingRequest>(raw_proxy_request);

        if let Ok(ref req) = request {
            assert_eq!(req.format_version, FORMAT_VERSION1);
        }

        request
            .context("deserialize proxy request")
            .map_err(anyhow::Error::from)
    }

    pub fn get_tpu_socket_addr(&self) -> SocketAddr {
        self.tpu_socket_addr
    }

    pub fn get_identity_tpunode(&self) -> Pubkey {
        self.identity_tpunode
    }

    pub fn get_transactions(&self) -> &[VersionedTransaction] {
        self.transactions.as_slice()
    }
}
