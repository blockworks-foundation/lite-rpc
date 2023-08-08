use anyhow::Context;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

///
/// lite-rpc to proxy wire format
/// compat info: non-public format ATM
/// initial version
pub const FORMAT_VERSION1: u16 = 2400;

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
            "TpuForwardingRequest for tpu target {} with identity {}: payload {} tx",
            &self.get_tpu_socket_addr(),
            &self.get_identity_tpunode(),
            &self.get_transaction_bytes().len()
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
            transactions: transactions.into_iter().map(Self::serialize).collect_vec(),
        }
    }

    fn serialize(tx: VersionedTransaction) -> TxData {
        TxData(tx.signatures[0], bincode::serialize(&tx).unwrap())
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

    pub fn get_transaction_bytes(&self) -> Vec<Vec<u8>> {
        self.transactions
            .iter()
            .map(|tx| tx.1.clone())
            .collect_vec()
    }

    pub fn get_hash(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        // note: assumes that there are transactions with >=0 signatures
        self.transactions[0].0.hash(&mut hasher);
        hasher.finish()
    }
}
