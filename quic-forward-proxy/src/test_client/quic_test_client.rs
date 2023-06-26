use std::net::{SocketAddr, SocketAddrV4};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use anyhow::bail;
use bytes::BufMut;
use log::info;
use quinn::{Endpoint, VarInt};
use rustls::ClientConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::{Transaction, VersionedTransaction};
use tokio::io::AsyncWriteExt;
use solana_lite_rpc_core::AnyhowJoinHandle;
use crate::quic_util::ALPN_TPU_FORWARDPROXY_PROTOCOL_ID;
use crate::tls_config_provicer::ProxyTlsConfigProvider;
use solana_lite_rpc_core::quic_connection_utils::SkipServerVerification;
use solana_lite_rpc_core::proxy_request_format::TpuForwardingRequest;
use crate::test_client::sample_data_factory::build_raw_sample_tx;

pub struct QuicTestClient {
    pub endpoint: Endpoint,
    pub proxy_addr: SocketAddr,
}

impl QuicTestClient {
    pub async fn new_with_endpoint(
        proxy_addr: SocketAddr,
        tls_config: &impl ProxyTlsConfigProvider
    ) -> anyhow::Result<Self> {
        let client_crypto = tls_config.get_client_tls_crypto_config();
        let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())?;
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(client_crypto)));

        Ok(Self { proxy_addr, endpoint })
    }

    // connect to a server
    pub async fn start_services(
        mut self,
    ) -> anyhow::Result<()> {
        let endpoint_copy = self.endpoint.clone();
        let test_client_service: AnyhowJoinHandle = tokio::spawn(async move {
            info!("Sample Quic Client starting ...");

            let mut ticker = tokio::time::interval(Duration::from_secs(3));
            // TODO exit signal
            loop {
                // create new connection everytime
                let connection_timeout = Duration::from_secs(5);
                let connecting = endpoint_copy.connect(self.proxy_addr, "localhost").unwrap();
                let connection = tokio::time::timeout(connection_timeout, connecting).await??;

                for si in 0..5 {
                    let (mut send, mut recv)  = connection.open_bi().await?;

                    let raw = build_memo_tx_raw();
                    info!("raw: {:02X?}", raw);
                    // send.write_all(format!("SAMPLE DATA on stream {}", si).as_bytes()).await?;

                    // shutdown stream
                    send.finish().await?;
                }

                connection.close(VarInt::from_u32(0), b"done");
                ticker.tick().await;
            }



            Ok(())
        });

        tokio::select! {
            res = test_client_service => {
                bail!("Sample client service exited unexpectedly {res:?}");
            },
        }
    }

}

fn build_tls_config() -> ClientConfig {
    // FIXME configured insecure https://quinn-rs.github.io/quinn/quinn/certificate.html
    let mut _roots = rustls::RootCertStore::empty();
    // TODO add certs

    let mut client_crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        // .with_root_certificates(roots)
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();
    client_crypto.enable_early_data = true;
    client_crypto.alpn_protocols = vec![ALPN_TPU_FORWARDPROXY_PROTOCOL_ID.to_vec()];

    return client_crypto;
}


fn build_memo_tx_raw() -> Vec<u8> {
    let payer_pubkey = Pubkey::new_unique();
    let signer_pubkey = Pubkey::new_unique();

    let memo_ix = spl_memo::build_memo("Hello world".as_bytes(), &[&signer_pubkey]);

    let tx = Transaction::new_with_payer(&[memo_ix], Some(&payer_pubkey));

    let wire_data = serialize_tpu_forwarding_request(
        "127.0.0.1:5454".parse().unwrap(),
        Pubkey::from_str("Bm8rtweCQ19ksNebrLY92H7x4bCaeDJSSmEeWqkdCeop").unwrap(),
        vec![tx.into()]);

    println!("wire_data: {:02X?}", wire_data);

    wire_data
}


fn serialize_tpu_forwarding_request(
    tpu_socket_addr: SocketAddr,
    tpu_identity: Pubkey,
    transactions: Vec<VersionedTransaction>) -> Vec<u8> {

    let request = TpuForwardingRequest::new(tpu_socket_addr, tpu_identity, transactions);

    bincode::serialize(&request).expect("Expect to serialize transactions")
}
