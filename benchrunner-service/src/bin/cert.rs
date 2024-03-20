use std::path::PathBuf;
use std::str::FromStr;
use native_tls::Identity;
use solana_lite_rpc_util::encoding::BinaryEncoding;
use anyhow::Context;

pub fn main() {
    let client_pks_b64 = std::fs::read_to_string(PathBuf::from_str("client_pks_b64.txt").unwrap()).unwrap();

    let client_pks = BinaryEncoding::Base64
        .decode(client_pks_b64)
        .context("client pks decode").unwrap();

    let client_pks =
        Identity::from_pkcs12(&client_pks, "p").unwrap();

}