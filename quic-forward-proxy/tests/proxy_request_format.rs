use solana_lite_rpc_quic_forward_proxy::proxy_request_format::TpuForwardingRequest;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::Transaction;
use std::str::FromStr;

#[test]
fn roundtrip() {
    let payer = Keypair::from_base58_string(
        "rKiJ7H5UUp3JR18kNyTF1XPuwPKHEM7gMLWHZPWP5djrW1vSjfwjhvJrevxF9MPmUmN9gJMLHZdLMgc9ao78eKr",
    );
    let payer_pubkey = payer.pubkey();

    let memo_ix = spl_memo::build_memo("Hello world".as_bytes(), &[&payer_pubkey]);

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
