use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use anyhow::{anyhow, bail, Error};
use async_trait::async_trait;
use dashmap::DashMap;
use itertools::{any, Itertools};
use log::{debug, error, info, trace, warn};
use quinn::{Connecting, Connection, ConnectionError, Endpoint, SendStream, ServerConfig, VarInt};
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
use crate::quic_connection_utils::{connection_stats, QuicConnectionError, QuicConnectionParameters, QuicConnectionUtils};
use crate::tls_config_provicer::{ProxyTlsConfigProvider, SelfSignedTlsConfigProvider};

const QUIC_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
pub const CONNECTION_RETRY_COUNT: usize = 10;

pub const MAX_TRANSACTIONS_PER_BATCH: usize = 10;
pub const MAX_BYTES_PER_BATCH: usize = 10;
const MAX_PARALLEL_STREAMS: usize = 6;

/// maintain many quic connection and streams to one TPU to send transactions - optimized for proxy use case
#[derive(Debug, Clone)]
pub struct TpuQuicClient {
    endpoint: Endpoint,
    // naive single non-recoverable connection - TODO moke it smarter
    // TODO consider using DashMap again
    connection_per_tpunode: Arc<RwLock<HashMap<SocketAddr, Connection>>>,
    last_stable_id: Arc<AtomicU64>,
}

impl TpuQuicClient {
    pub async fn create_connection(&self, tpu_address: SocketAddr) -> anyhow::Result<Connection> {
        let connection =
            // TODO try 0rff
            match QuicConnectionUtils::make_connection_0rtt(
                self.endpoint.clone(), tpu_address, QUIC_CONNECTION_TIMEOUT)
                .await {
                Ok(conn) => conn,
                Err(err) => {
                    warn!("Failed to open Quic connection to TPU {}: {}", tpu_address, err);
                    return Err(anyhow!("Failed to create Quic connection to TPU {}: {}", tpu_address, err));
                },
            };

        Ok(connection)
    }
}

/// per TPU connection manager
#[async_trait]
pub trait SingleTPUConnectionManager {
    // async fn refresh_connection(&self, connection: &Connection) -> Connection;
    async fn get_or_create_connection(&self, tpu_address: SocketAddr) -> anyhow::Result<Connection>;
    fn update_last_stable_id(&self, stable_id: u64);
}

pub type SingleTPUConnectionManagerWrapper = dyn SingleTPUConnectionManager + Sync + Send;

#[async_trait]
impl SingleTPUConnectionManager for TpuQuicClient {

    // make sure the connection is usable for a resonable time
    // never returns the "same" instances but a clone
    // async fn refresh_connection(&self, connection: &Connection) -> Connection {
    //     let reverse_lookup = self.connection_per_tpunode.into_read_only().values().find(|conn| {
    //         conn.stable_id() == connection.stable_id()
    //     });
    //
    //     match reverse_lookup {
    //         Some(existing_conn) => {
    //             return existing_conn.clone();
    //         }
    //         None => {
    //             TpuQuicClient::create_new(&self, reverse_lookup).await.unwrap())
    //         }
    //     }
    //
    //     // TODO implement
    //     connection.clone()
    // }

    #[tracing::instrument(skip(self), level = "debug")]
    // TODO improve error handling; might need to signal if connection was reset
    async fn get_or_create_connection(&self, tpu_address: SocketAddr) -> anyhow::Result<Connection> {
        // TODO try 0rff
        // QuicConnectionUtils::make_connection(
        //     self.endpoint.clone(), tpu_address, QUIC_CONNECTION_TIMEOUT)
        //     .await.unwrap()


        {
            if let Some(conn) =  self.connection_per_tpunode.read().await.get(&tpu_address) {
                debug!("reusing connection {} for tpu {}; last_stable_id is {}",
                    conn.stable_id(), tpu_address, self.last_stable_id.load(Ordering::Relaxed));
                return Ok(conn.clone());
            }
        }

        let connection =
            // TODO try 0rff
            match QuicConnectionUtils::make_connection_0rtt(
                self.endpoint.clone(), tpu_address, QUIC_CONNECTION_TIMEOUT)
                .await {
                Ok(conn) => conn,
                Err(err) => {
                    warn!("Failed to open Quic connection to TPU {}: {}", tpu_address, err);
                    return Err(anyhow!("Failed to create Quic connection to TPU {}: {}", tpu_address, err));
                },
            };

        let mut lock = self.connection_per_tpunode.write().await;
        let old_value = lock.insert(tpu_address, connection.clone());
        assert!(old_value.is_none(), "no prev value must be overridden");

        debug!("Created new Quic connection {} to TPU node {}, total connections is now {}",
            connection.stable_id(), tpu_address, lock.len());
        return Ok(connection);
    }

    fn update_last_stable_id(&self, stable_id: u64) {
        self.last_stable_id.store(stable_id, Ordering::Relaxed);
    }

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

        let endpoint_outbound = QuicConnectionUtils::create_tpu_client_endpoint(certificate.clone(), key.clone());

        let active_tpu_connection = TpuQuicClient {
            endpoint: endpoint_outbound.clone(),
            connection_per_tpunode: Arc::new(RwLock::new(HashMap::new())),
            last_stable_id: Arc::new(AtomicU64::new(0)),
        };

        active_tpu_connection
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub async fn send_txs_to_tpu(&self,
                                 tpu_address: SocketAddr,
                                 txs: &Vec<VersionedTransaction>,
                                 exit_signal: Arc<AtomicBool>,
    ) {

        if false {
            // note: this impl does not deal with connection errors
            // throughput_50 493.70 tps
            // throughput_50 769.43 tps (with finish timeout)
            // TODO join get_or_create_connection future and read_to_end
            // TODO add error handling
            let tpu_connection = self.get_or_create_connection(tpu_address).await.unwrap();

            for chunk in txs.chunks(MAX_PARALLEL_STREAMS) {
                let vecvec = chunk.iter().map(|tx| {
                    let tx_raw = bincode::serialize(tx).unwrap();
                    tx_raw
                }).collect_vec();
                QuicConnectionUtils::send_transaction_batch_parallel(
                    tpu_connection.clone(),
                    vecvec,
                    exit_signal.clone(),
                    QUIC_CONNECTION_TIMEOUT,
                ).await;
            }
        } else {
            // throughput_50 676.65 tps
            let connection_params = QuicConnectionParameters {
                connection_retry_count: 10,
                finalize_timeout: Duration::from_millis(200),
                unistream_timeout: Duration::from_millis(500),
                write_timeout: Duration::from_secs(1),
            };

            let connection_manager = self as &SingleTPUConnectionManagerWrapper;

            // TODO connection_params should be part of connection_manager
            Self::send_transaction_batch(serialize_to_vecvec(&txs), tpu_address, exit_signal, connection_params, connection_manager).await;

        }

    }

    pub async fn send_transaction_batch(txs: Vec<Vec<u8>>,
                                        tpu_address: SocketAddr,
                                        exit_signal: Arc<AtomicBool>,
                                        // _timeout_counters: Arc<AtomicU64>,
                                        // last_stable_id: Arc<AtomicU64>,
                                        connection_params: QuicConnectionParameters,
                                        connection_manager: &SingleTPUConnectionManagerWrapper,
    ) {
        let mut queue = VecDeque::new();
        for tx in txs {
            queue.push_back(tx);
        }
        info!("send_transaction_batch: queue size is {}", queue.len());
        let connection_retry_count = connection_params.connection_retry_count;
        for _ in 0..connection_retry_count {
            if queue.is_empty() || exit_signal.load(Ordering::Relaxed) {
                // return
                return;
            }

            let mut do_retry = false;
            while !queue.is_empty() {
                let tx = queue.pop_front().unwrap();
                let connection = connection_manager.get_or_create_connection(tpu_address).await;

                if exit_signal.load(Ordering::Relaxed) {
                    return;
                }

                if let Ok(connection) = connection {
                    let current_stable_id = connection.stable_id() as u64;
                    match QuicConnectionUtils::open_unistream(
                        &connection,
                        connection_params.unistream_timeout,
                    )
                        .await
                    {
                        Ok(send_stream) => {
                            match QuicConnectionUtils::write_all(
                                send_stream,
                                &tx,
                                connection_params,
                            )
                                .await
                            {
                                Ok(()) => {
                                    // do nothing
                                    debug!("connection stats (proxy send tx batch): {}", connection_stats(&connection));
                                }
                                Err(QuicConnectionError::ConnectionError { retry }) => {
                                    do_retry = retry;
                                }
                                Err(QuicConnectionError::TimeOut) => {
                                    // timeout_counters.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(QuicConnectionError::ConnectionError { retry }) => {
                            do_retry = retry;
                        }
                        Err(QuicConnectionError::TimeOut) => {
                            // timeout_counters.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if do_retry {
                        connection_manager.update_last_stable_id(current_stable_id);

                        queue.push_back(tx);
                        break;
                    }
                } else {
                    warn!(
                        "Could not establish connection with {}",
                        "identity"
                    );
                    break;
                }
            }
            if !do_retry {
                break;
            }
        }
    }

}


fn serialize_to_vecvec(transactions: &Vec<VersionedTransaction>) -> Vec<Vec<u8>> {
    transactions.iter().map(|tx| {
        let tx_raw = bincode::serialize(tx).unwrap();
        tx_raw
    }).collect_vec()
}


// send potentially large amount of transactions to a single TPU
pub async fn send_txs_to_tpu_static(
    tpu_connection: Connection,
    tpu_address: SocketAddr,
    txs: &Vec<VersionedTransaction>,
    exit_signal: Arc<AtomicBool>,
) {

    // note: this impl does not deal with connection errors
    // throughput_50 493.70 tps
    // throughput_50 769.43 tps (with finish timeout)
    // TODO join get_or_create_connection future and read_to_end
    // TODO add error handling

    for chunk in txs.chunks(MAX_PARALLEL_STREAMS) {
        let vecvec = chunk.iter().map(|tx| {
            let tx_raw = bincode::serialize(tx).unwrap();
            tx_raw
        }).collect_vec();
        QuicConnectionUtils::send_transaction_batch_parallel(
            tpu_connection.clone(),
            vecvec,
            exit_signal.clone(),
            QUIC_CONNECTION_TIMEOUT,
        ).await;
    }

}