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
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use tokio::sync::RwLock;
use crate::quic_connection_utils::QuicConnectionUtils;
use crate::tls_config_provicer::{ProxyTlsConfigProvider, SelfSignedTlsConfigProvider};

pub const QUIC_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
pub const CONNECTION_RETRY_COUNT: usize = 10;

/// stable connect to TPU to send transactions - optimized for proxy use case
#[derive(Clone)]
pub struct TpuQuicConnection {
    endpoint: Endpoint,
}

impl TpuQuicConnection {

    /// takes a validator identity and creates a new QUIC client; appears as staked peer to TPU
    // note: ATM the provided identity might or might not be a valid validator keypair
    pub fn new_with_validator_identity(validator_identity: &Keypair) -> TpuQuicConnection {
        let (certificate, key) = new_self_signed_tls_certificate(
            validator_identity,
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        )
            .expect("Failed to initialize QUIC connection certificates");

        let endpoint_outbound = QuicConnectionUtils::create_endpoint(certificate.clone(), key.clone());

        let active_tpu_connection = TpuQuicConnection {
            endpoint: endpoint_outbound.clone(),
        };

        active_tpu_connection
    }

    pub async fn send_txs_to_tpu(&self,
                                 exit_signal: Arc<AtomicBool>,
                                 validator_identity: Arc<Keypair>,
                                 tpu_identity: Pubkey,
                                 tpu_address: SocketAddr,
                                 txs: &Vec<VersionedTransaction>) {

        let last_stable_id: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

        let txs_raw = serialize_to_vecvec(&txs);

        info!("received vecvec: {}", txs_raw.iter().map(|tx| tx.len().to_string()).into_iter().join(","));

        let connection =
            Arc::new(RwLock::new(
                QuicConnectionUtils::connect(
                    tpu_identity,
                    false,
                    self.endpoint.clone(),
                    tpu_address,
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
            self.endpoint.clone(),
            tpu_address,
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

}


fn serialize_to_vecvec(transactions: &Vec<VersionedTransaction>) -> Vec<Vec<u8>> {
    transactions.iter().map(|tx| {
        let tx_raw = bincode::serialize(tx).unwrap();
        tx_raw
    }).collect_vec()
}
