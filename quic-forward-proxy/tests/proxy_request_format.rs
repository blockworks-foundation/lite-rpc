
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
