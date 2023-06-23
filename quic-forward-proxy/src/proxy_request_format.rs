use std::net::SocketAddrV4;
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


// TODO reame
fn deserialize_request(raw_tx: &Vec<u8>) -> TpuForwardingRequest {
    todo!();
}

fn serialize_transactions_for_tpu(
    tpu_socket_addr: SocketAddrV4,
    tpu_identity: Pubkey,
    transactions: Vec<VersionedTransaction>) -> Vec<u8> {

    let request = TpuForwardingRequest::new(tpu_socket_addr, tpu_identity, transactions);

    bincode::serialize(&request).expect("Expect to serialize transactions")
}


mod test {
    use std::str::FromStr;
    use log::info;
    use solana_sdk::pubkey::Pubkey;
    use crate::proxy_request_format::serialize_transactions_for_tpu;

    #[test]
    fn deser() {
        let wire_data = serialize_transactions_for_tpu(
            "127.0.0.1:5454".parse().unwrap(),
            Pubkey::from_str("Bm8rtweCQ19ksNebrLY92H7x4bCaeDJSSmEeWqkdCeop").unwrap(),
            vec![]);

        println!("wire_data: {:02X?}", wire_data);

    }
}

