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
const FORMAT_VERSION1: u16 = 2500;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TxData(Signature, Vec<u8>);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TpuNode {
    pub tpu_socket_addr: SocketAddr,
    pub identity_tpunode: Pubkey, // note: this is only used for debugging
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TpuForwardingRequest {
    format_version: u16,
    tpu_nodes: Vec<TpuNode>,
    transactions: Vec<TxData>,
}

impl Display for TpuForwardingRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TpuForwardingRequest t9 {} tpu nodes",
            &self.tpu_nodes.len(),
        )
    }
}

impl TpuForwardingRequest {
    pub fn new(
        tpu_fanout_nodes: &[(SocketAddr, Pubkey)],
        transactions: Vec<VersionedTransaction>,
    ) -> Self {
        TpuForwardingRequest {
            format_version: FORMAT_VERSION1,
            tpu_nodes: tpu_fanout_nodes
                .iter()
                .map(|(tpu_addr, identity)| TpuNode {
                    tpu_socket_addr: *tpu_addr,
                    identity_tpunode: *identity,
                })
                .collect_vec(),
            transactions: transactions
                .iter()
                .map(|tx| TxData(tx.signatures[0], bincode::serialize(tx).unwrap()))
                .collect_vec(),
        }
    }

    pub fn try_serialize_wire_format(&self) -> anyhow::Result<Vec<u8>> {
        bincode::serialize(&self)
            .context("serialize proxy request")
            .map_err(anyhow::Error::from)
    }

    pub fn get_tpu_nodes(&self) -> &Vec<TpuNode> {
        &self.tpu_nodes
    }
}
