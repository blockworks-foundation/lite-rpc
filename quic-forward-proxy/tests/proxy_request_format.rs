use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::str::FromStr;
use anyhow::Context;
use bincode::DefaultOptions;
use log::info;
use serde::Serialize;
use solana_sdk::hash::{Hash, Hasher};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::{Transaction, VersionedTransaction};
use spl_memo::solana_program::message::VersionedMessage;
use solana_lite_rpc_quic_forward_proxy::proxy_request_format::TpuForwardingRequest;

#[test]
fn roundtrip() {

    let payer = Keypair::from_base58_string("rKiJ7H5UUp3JR18kNyTF1XPuwPKHEM7gMLWHZPWP5djrW1vSjfwjhvJrevxF9MPmUmN9gJMLHZdLMgc9ao78eKr");
    let payer_pubkey = payer.pubkey();

    let memo_ix = spl_memo::build_memo("Hello world".as_bytes(), &[&payer_pubkey]);

    let tx = Transaction::new_with_payer(&[memo_ix], Some(&payer_pubkey));

    let wire_data = TpuForwardingRequest::new(
        "127.0.0.1:5454".parse().unwrap(),
        Pubkey::from_str("Bm8rtweCQ19ksNebrLY92H7x4bCaeDJSSmEeWqkdCeop").unwrap(),
        vec![tx.into()]
    ).serialize_wire_format();

    println!("wire_data: {:02X?}", wire_data);

    let request = TpuForwardingRequest::deserialize_from_raw_request(&wire_data);

    assert_eq!(request.get_tpu_socket_addr().is_ipv4(), true);
    assert_eq!(request.get_transactions().len(), 1);

}


