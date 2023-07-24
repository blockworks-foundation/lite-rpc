use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Duration;
use anyhow::{anyhow, bail};
use dashmap::DashMap;
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

const QUIC_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
pub const CONNECTION_RETRY_COUNT: usize = 10;

/// stable connect to TPU to send transactions - optimized for proxy use case
#[derive(Debug, Clone)]
pub struct TpuQuicClient {
    endpoint: Endpoint,
    // naive single non-recoverable connection - TODO moke it smarter
    connection_per_tpunode: Arc<DashMap<SocketAddr, Connection>>,
}

impl TpuQuicClient {

    /// takes a validator identity and creates a new QUIC client; appears as staked peer to TPU
    // note: ATM the provided identity might or might not be a valid validator keypair
    pub async fn new_with_validator_identity(validator_identity: &Keypair) -> TpuQuicClient {
        info!("Setup TPU Quic stable connection ...");
        let (certificate, key) = new_self_signed_tls_certificate(
            validator_identity,
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        )
            .expect("Failed to initialize QUIC connection certificates");

        let endpoint_outbound = QuicConnectionUtils::create_endpoint(certificate.clone(), key.clone());

        let active_tpu_connection = TpuQuicClient {
            endpoint: endpoint_outbound.clone(),
            connection_per_tpunode: Arc::new(DashMap::new()),
        };

        active_tpu_connection
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn get_or_create_connection(&self, tpu_address: SocketAddr) -> Connection {
        if let Some(conn) = self.connection_per_tpunode.get(&tpu_address) {
            return conn.clone();
        }

        let connection =
        // TODO try 0rff
            QuicConnectionUtils::make_connection(
                self.endpoint.clone(), tpu_address, QUIC_CONNECTION_TIMEOUT)
                .await.unwrap();


        self.connection_per_tpunode.insert(tpu_address, connection.clone());

        debug!("Created new Quic connection to TPU node {}, total connections is now {}", tpu_address, self.connection_per_tpunode.len());
        return connection;
    }

    pub async fn send_txs_to_tpu(&self,
                                 connection: Connection,
                                 txs: &Vec<VersionedTransaction>,
                                 exit_signal: Arc<AtomicBool>,
    ) {
        QuicConnectionUtils::send_transaction_batch(
            connection,
            serialize_to_vecvec(txs),
            exit_signal.clone(),
            QUIC_CONNECTION_TIMEOUT,
        ).await;

    }

}


fn serialize_to_vecvec(transactions: &Vec<VersionedTransaction>) -> Vec<Vec<u8>> {
    transactions.iter().map(|tx| {
        let tx_raw = bincode::serialize(tx).unwrap();
        tx_raw
    }).collect_vec()
}
