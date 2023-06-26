use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::str::FromStr;
use anyhow::Context;
use bincode::DefaultOptions;
use log::info;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::{Transaction, VersionedTransaction};
use spl_memo::solana_program::message::VersionedMessage;
use solana_lite_rpc_core::proxy_request_format::TpuForwardingRequest;
use solana_lite_rpc_core::proxy_request_format::*;

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

    assert_eq!(request.get_tpu_socket_addr().is_ipv4(), true);
    assert_eq!(request.get_transactions().len(), 1);

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

