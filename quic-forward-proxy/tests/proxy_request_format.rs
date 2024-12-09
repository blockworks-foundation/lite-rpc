use std::str::FromStr;

use solana_sdk::address_lookup_table::program::id;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::Transaction;

use solana_lite_rpc_quic_forward_proxy::proxy_request_format::TpuForwardingRequest;

#[test]
fn roundtrip() {
    let payer_pubkey = Pubkey::from_str("Bm8rtweCQ19ksNebrLY92H7x4bCaeDJSSmEeWqkdCeop").unwrap();

    let memo_ix = Instruction {
        program_id: id(),
        accounts: vec![AccountMeta::new_readonly(payer_pubkey, true)],
        data: Vec::new(),
    };

    let tx = Transaction::new_with_payer(&[memo_ix], Some(&payer_pubkey));

    let wire_data = TpuForwardingRequest::new(
        vec![(
            "127.0.0.1:5454".parse().unwrap(),
            Pubkey::from_str("Bm8rtweCQ19ksNebrLY92H7x4bCaeDJSSmEeWqkdCeop").unwrap(),
        )],
        vec![tx.into()],
    )
    .try_serialize_wire_format()
    .unwrap();

    println!("wire_data: {:02X?}", wire_data);

    let request = TpuForwardingRequest::try_deserialize_from_wire_format(&wire_data).unwrap();

    assert_eq!(request.get_tpu_nodes().len(), 1);
    assert!(request.get_tpu_nodes()[0].tpu_socket_addr.is_ipv4());
    assert_eq!(request.get_transaction_bytes().len(), 1);
}

#[test]
fn deserialize_error() {
    let value: &[u8] = &[1, 2, 3, 4];
    let result = TpuForwardingRequest::try_deserialize_from_wire_format(value);
    assert_eq!(result.unwrap_err().to_string(), "deserialize proxy request");
}
