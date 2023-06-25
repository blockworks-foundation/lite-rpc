use std::net::SocketAddrV4;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TpuForwardingRequestV1 {
    pub tpu_socket_addr: SocketAddrV4, // TODO is that correct
    pub identity_tpunode: Pubkey,
    pub transactions: Vec<VersionedTransaction>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TpuForwardingRequest {
    V1(TpuForwardingRequestV1),
}

impl TpuForwardingRequest {
    pub fn new(tpu_socket_addr: SocketAddrV4, identity_tpunode: Pubkey,
               transactions: Vec<VersionedTransaction>) -> Self {
        TpuForwardingRequest::V1(
            TpuForwardingRequestV1 {
                tpu_socket_addr,
                identity_tpunode,
                transactions,
            })
    }
}

fn serialize_tpu_forwarding_request(
    tpu_socket_addr: SocketAddrV4,
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

mod test {
    use std::str::FromStr;
    use log::info;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::transaction::Transaction;
    use spl_memo::solana_program::message::VersionedMessage;
    use crate::proxy_request_format::*;

    #[test]
    fn roundtrip() {

        let payer_pubkey = Pubkey::new_unique();
        let signer_pubkey = Pubkey::new_unique();

        let memo_ix = spl_memo::build_memo("Hello world".as_bytes(), &[&signer_pubkey]);

        let tx = Transaction::new_with_payer(&[memo_ix], Some(&payer_pubkey));

        let wire_data = serialize_tpu_forwarding_request(
            "127.0.0.1:5454".parse().unwrap(),
            Pubkey::from_str("Bm8rtweCQ19ksNebrLY92H7x4bCaeDJSSmEeWqkdCeop").unwrap(),
            vec![tx.into()]);

        println!("wire_data: {:02X?}", wire_data);

        let request = deserialize_tpu_forwarding_request(&wire_data);

        let TpuForwardingRequest::V1(req1) = request;

        assert_eq!(req1.transactions.len(), 1);

    }
}
