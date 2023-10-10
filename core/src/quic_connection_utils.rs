use log::{info, trace, warn};
use quinn::{
    ClientConfig, Connection, ConnectionError, Endpoint, EndpointConfig, IdleTimeout, SendStream,
    TokioRuntime, TransportConfig,
};
use solana_sdk::pubkey::Pubkey;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::timeout;

const ALPN_TPU_PROTOCOL_ID: &[u8] = b"solana-tpu";

pub enum QuicConnectionError {
    TimeOut,
    ConnectionError { retry: bool },
}

#[derive(Clone, Copy)]
pub struct QuicConnectionParameters {
    pub connection_timeout: Duration,
    pub unistream_timeout: Duration,
    pub write_timeout: Duration,
    pub finalize_timeout: Duration,
    pub connection_retry_count: usize,
    pub max_number_of_connections: usize,
    pub number_of_transactions_per_unistream: usize,
}

pub struct QuicConnectionUtils {}

impl QuicConnectionUtils {
    pub fn create_endpoint(certificate: rustls::Certificate, key: rustls::PrivateKey) -> Endpoint {
        let mut endpoint = {
            let client_socket =
                solana_net_utils::bind_in_range(IpAddr::V4(Ipv4Addr::UNSPECIFIED), (8000, 10000))
                    .expect("create_endpoint bind_in_range")
                    .1;
            let config = EndpointConfig::default();
            quinn::Endpoint::new(config, None, client_socket, TokioRuntime)
                .expect("create_endpoint quinn::Endpoint::new")
        };

        let mut crypto = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_single_cert(vec![certificate], key)
            .expect("Failed to set QUIC client certificates");

        crypto.enable_early_data = true;
        crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

        let mut config = ClientConfig::new(Arc::new(crypto));
        let mut transport_config = TransportConfig::default();

        let timeout = IdleTimeout::try_from(Duration::from_secs(1)).unwrap();
        transport_config.max_idle_timeout(Some(timeout));
        transport_config.keep_alive_interval(Some(Duration::from_millis(500)));
        apply_gso_workaround(&mut transport_config);
        config.transport_config(Arc::new(transport_config));

        endpoint.set_default_client_config(config);

        endpoint
    }

    pub async fn make_connection(
        endpoint: Endpoint,
        addr: SocketAddr,
        connection_timeout: Duration,
    ) -> anyhow::Result<Connection> {
        let connecting = endpoint.connect(addr, "connect")?;
        let res = timeout(connection_timeout, connecting).await??;
        Ok(res)
    }

    pub async fn make_connection_0rtt(
        endpoint: Endpoint,
        addr: SocketAddr,
        connection_timeout: Duration,
    ) -> anyhow::Result<Connection> {
        let connecting = endpoint.connect(addr, "connect")?;
        let connection = match connecting.into_0rtt() {
            Ok((connection, zero_rtt)) => {
                if (timeout(connection_timeout, zero_rtt).await).is_ok() {
                    connection
                } else {
                    return Err(ConnectionError::TimedOut.into());
                }
            }
            Err(connecting) => {
                if let Ok(connecting_result) = timeout(connection_timeout, connecting).await {
                    connecting_result?
                } else {
                    return Err(ConnectionError::TimedOut.into());
                }
            }
        };
        Ok(connection)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        identity: Pubkey,
        already_connected: bool,
        endpoint: Endpoint,
        addr: SocketAddr,
        connection_timeout: Duration,
        connection_retry_count: usize,
        exit_signal: Arc<AtomicBool>,
    ) -> Option<Connection> {
        for _ in 0..connection_retry_count {
            let conn = if already_connected {
                Self::make_connection_0rtt(endpoint.clone(), addr, connection_timeout).await
            } else {
                Self::make_connection(endpoint.clone(), addr, connection_timeout).await
            };
            match conn {
                Ok(conn) => {
                    return Some(conn);
                }
                Err(e) => {
                    trace!("Could not connect to {} because of error {}", identity, e);
                    if exit_signal.load(Ordering::Relaxed) {
                        break;
                    }
                }
            }
        }
        None
    }

    pub async fn write_all(
        mut send_stream: SendStream,
        tx: &Vec<u8>,
        identity: Pubkey,
        connection_params: QuicConnectionParameters,
    ) -> Result<(), QuicConnectionError> {
        let write_timeout_res = timeout(
            connection_params.write_timeout,
            send_stream.write_all(tx.as_slice()),
        )
        .await;
        match write_timeout_res {
            Ok(write_res) => {
                if let Err(e) = write_res {
                    trace!(
                        "Error while writing transaction for {}, error {}",
                        identity,
                        e
                    );
                    return Err(QuicConnectionError::ConnectionError { retry: true });
                }
            }
            Err(_) => {
                warn!("timeout while writing transaction for {}", identity);
                return Err(QuicConnectionError::TimeOut);
            }
        }

        let finish_timeout_res =
            timeout(connection_params.finalize_timeout, send_stream.finish()).await;
        match finish_timeout_res {
            Ok(finish_res) => {
                if let Err(e) = finish_res {
                    trace!(
                        "Error while finishing transaction for {}, error {}",
                        identity,
                        e
                    );
                    return Err(QuicConnectionError::ConnectionError { retry: false });
                }
            }
            Err(_) => {
                warn!("timeout while finishing transaction for {}", identity);
                return Err(QuicConnectionError::TimeOut);
            }
        }

        Ok(())
    }

    pub async fn open_unistream(
        connection: Connection,
        connection_timeout: Duration,
    ) -> Result<SendStream, QuicConnectionError> {
        match timeout(connection_timeout, connection.open_uni()).await {
            Ok(Ok(unistream)) => Ok(unistream),
            Ok(Err(_)) => Err(QuicConnectionError::ConnectionError { retry: true }),
            Err(_) => Err(QuicConnectionError::TimeOut),
        }
    }
}

pub struct SkipServerVerification;

impl SkipServerVerification {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

// connection for sending proxy request: FrameStats {
// ACK: 2, CONNECTION_CLOSE: 0, CRYPTO: 3, DATA_BLOCKED: 0, DATAGRAM: 0, HANDSHAKE_DONE: 1,
// MAX_DATA: 0, MAX_STREAM_DATA: 1, MAX_STREAMS_BIDI: 0, MAX_STREAMS_UNI: 0, NEW_CONNECTION_ID: 4,
// NEW_TOKEN: 0, PATH_CHALLENGE: 0, PATH_RESPONSE: 0, PING: 0, RESET_STREAM: 0, RETIRE_CONNECTION_ID: 1,
// STREAM_DATA_BLOCKED: 0, STREAMS_BLOCKED_BIDI: 0, STREAMS_BLOCKED_UNI: 0, STOP_SENDING: 0, STREAM: 0 }
// rtt=1.08178ms
pub fn connection_stats(connection: &Connection) -> String {
    // see https://www.rfc-editor.org/rfc/rfc9000.html#name-frame-types-and-formats
    format!(
        "stable_id {}, rtt={:?}, stats {:?}",
        connection.stable_id(),
        connection.stats().path.rtt,
        connection.stats().frame_rx
    )
}

/// env flag to optionally disable GSO (generic segmentation offload) on environments where Quinn cannot detect it properly
/// see https://github.com/quinn-rs/quinn/pull/1671
pub fn apply_gso_workaround(tc: &mut TransportConfig) {
    if disable_gso() {
        tc.enable_segmentation_offload(false);
    }
}

pub fn log_gso_workaround() {
    info!("GSO force-disabled? {}", disable_gso());
}

/// note: true means that quinn's heuristic for GSO detection is used to decide if GSO is used
fn disable_gso() -> bool {
    std::env::var("DISABLE_GSO")
        .unwrap_or("false".to_string())
        .parse::<bool>()
        .expect("flag must be true or false")
}
