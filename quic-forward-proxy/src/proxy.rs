use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Duration;
use anyhow::{anyhow, bail};
use itertools::{any, Itertools};
use log::{debug, error, info, trace, warn};
use quinn::{Connecting, Connection, Endpoint, SendStream, ServerConfig, VarInt};
use rcgen::generate_simple_self_signed;
use rustls::{Certificate, PrivateKey};
use rustls::server::ResolvesServerCert;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::VersionedTransaction;
use tokio::net::ToSocketAddrs;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use tokio::sync::RwLock;
use solana_lite_rpc_core::proxy_request_format::TpuForwardingRequest;
use solana_lite_rpc_core::quic_connection_utils::QuicConnectionUtils;
use solana_lite_rpc_services::tpu_utils::tpu_connection_manager::{ActiveConnection, CONNECTION_RETRY_COUNT, QUIC_CONNECTION_TIMEOUT};
use crate::tls_config_provicer::{ProxyTlsConfigProvider, SelfSignedTlsConfigProvider};


pub struct QuicForwardProxy {
    endpoint: Endpoint,
    validator_identity: Arc<Keypair>,
}

impl QuicForwardProxy {
    pub async fn new(
        proxy_listener_addr: SocketAddr,
        tls_config: &SelfSignedTlsConfigProvider,
        validator_identity: Arc<Keypair>) -> anyhow::Result<Self> {
        let server_tls_config = tls_config.get_server_tls_crypto_config();

        let mut quinn_server_config = ServerConfig::with_crypto(Arc::new(server_tls_config));

        let endpoint = Endpoint::server(quinn_server_config, proxy_listener_addr).unwrap();
        info!("tpu forward proxy listening on {}", endpoint.local_addr()?);
        info!("staking from validator identity {}", validator_identity.pubkey());

        Ok(Self {endpoint, validator_identity })

    }

    pub async fn start_services(
        mut self,
    ) -> anyhow::Result<()> {
        let exit_signal = Arc::new(AtomicBool::new(false));

        let endpoint = self.endpoint.clone();
        let quic_proxy: AnyhowJoinHandle = tokio::spawn(async move {
            info!("TPU Quic Proxy server start on {}", endpoint.local_addr()?);

            let identity_keypair = Keypair::new(); // TODO

            while let Some(conn) = endpoint.accept().await {
                trace!("connection incoming");
                let fut = handle_connection(conn, exit_signal.clone(), self.validator_identity.clone());
                tokio::spawn(async move {
                    if let Err(e) = fut.await {
                        error!("connection failed: {reason}", reason = e.to_string())
                    }
                });
            }

            bail!("TPU Quic Proxy server stopped");
        });

        tokio::select! {
            res = quic_proxy => {
                bail!("TPU Quic Proxy server exited unexpectedly {res:?}");
            },
        }
    }

}


async fn handle_connection(connecting: Connecting, exit_signal: Arc<AtomicBool>, validator_identity: Arc<Keypair>) -> anyhow::Result<()> {
    let connection = connecting.await?;
    debug!("inbound connection established, remote {connection}", connection = connection.remote_address());
    loop {
        let maybe_stream = connection.accept_uni().await;
        let mut recv_stream = match maybe_stream {
            Err(quinn::ConnectionError::ApplicationClosed(reason)) => {
                debug!("connection closed by peer - reason: {:?}", reason);
                if reason.error_code != VarInt::from_u32(0) {
                    return Err(anyhow!("connection closed by peer with unexpected reason: {:?}", reason));
                }
                debug!("connection gracefully closed by peer");
                return Ok(());
            },
            Err(e) => {
                error!("failed to accept stream: {}", e);
                return Err(anyhow::Error::msg("error accepting stream"));
            }
            Ok(s) => s,
        };
        let exit_signal_copy = exit_signal.clone();
        let validator_identity_copy = validator_identity.clone();
        tokio::spawn(async move {
            let raw_request = recv_stream.read_to_end(10_000_000).await
                .unwrap();
            debug!("read proxy_request {} bytes", raw_request.len());

            let proxy_request = TpuForwardingRequest::deserialize_from_raw_request(&raw_request);

            debug!("proxy request details: {}", proxy_request);
            let tpu_identity = proxy_request.get_identity_tpunode();
            let tpu_addr = proxy_request.get_tpu_socket_addr();
            let txs = proxy_request.get_transactions();

            send_txs_to_tpu(exit_signal_copy, validator_identity_copy, tpu_identity, tpu_addr, &txs).await;

        });

    } // -- loop
}

mod test {
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use solana_sdk::pubkey::Pubkey;
    use crate::cli::get_identity_keypair;
    use crate::proxy::send_txs_to_tpu;

    #[test]
    fn call() {
        let exit_signal = Arc::new(AtomicBool::new(false));

        let validator_identity = get_identity_keypair(&"/Users/stefan/mango/projects/quic-forward-proxy/local-testvalidator-stake-account.json".to_string());
        let tpu_identity = Pubkey::from_str("asdfsdf").unwrap();
        let tpu_address = "127.0.0.1:1027".parse().unwrap();
        send_txs_to_tpu(exit_signal, validator_identity, tpu_identity, tpu_address, &vec![])

    }
}

async fn send_txs_to_tpu(exit_signal: Arc<AtomicBool>, validator_identity: Arc<Keypair>, tpu_identity: Pubkey, tpu_addr: SocketAddr, txs: &Vec<VersionedTransaction>) {
    let (certificate, key) = new_self_signed_tls_certificate(
        validator_identity.as_ref(),
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
    )
        .expect("Failed to initialize QUIC client certificates");

    let endpoint = QuicConnectionUtils::create_endpoint(certificate.clone(), key.clone());
    let last_stable_id: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

    let txs_raw = serialize_to_vecvec(&txs);

    info!("received vecvec: {}", txs_raw.iter().map(|tx| tx.len().to_string()).into_iter().join(","));

    let connection =
        Arc::new(RwLock::new(
            QuicConnectionUtils::connect(
                tpu_identity,
                false,
                endpoint.clone(),
                tpu_addr,
                QUIC_CONNECTION_TIMEOUT,
                CONNECTION_RETRY_COUNT,
                exit_signal.clone(),
                || {
                    // do nothing
                },
            ).await.unwrap()));

    QuicConnectionUtils::send_transaction_batch(
        connection.clone(),
        txs_raw,
        tpu_identity,
        endpoint,
        tpu_addr,
        exit_signal.clone(),
        last_stable_id,
        QUIC_CONNECTION_TIMEOUT,
        CONNECTION_RETRY_COUNT,
        || {
            // do nothing
        }
    ).await;

    {
        let conn = connection.clone();
        conn.write().await.close(0u32.into(), b"done");
    }
}

fn serialize_to_vecvec(transactions: &Vec<VersionedTransaction>) -> Vec<Vec<u8>> {
    transactions.iter().map(|tx| {
        let tx_raw = bincode::serialize(tx).unwrap();
        tx_raw
    }).collect_vec()
}
