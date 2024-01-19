use log::trace;
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use quinn::{
    ClientConfig, Connection, ConnectionError, Endpoint, EndpointConfig, IdleTimeout, SendStream,
    TokioRuntime, TransportConfig, VarInt,
};
use solana_lite_rpc_core::network_utils::apply_gso_workaround;
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

lazy_static::lazy_static! {
    static ref NB_QUIC_0RTT_TIMEOUT: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_0RTT_timedout", "Number of times 0RTT timedout")).unwrap();
    static ref NB_QUIC_CONNECTION_TIMEOUT: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_connection_timedout", "Number of times connection timedout")).unwrap();
    static ref NB_QUIC_CONNECTION_ERRORED: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_connection_errored", "Number of times connection errored")).unwrap();
    static ref NB_QUIC_WRITEALL_TIMEOUT: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_writeall_timedout", "Number of times writeall timedout")).unwrap();
    static ref NB_QUIC_WRITEALL_ERRORED: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_writeall_errored", "Number of times writeall errored")).unwrap();
    static ref NB_QUIC_FINISH_TIMEOUT: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_finish_timedout", "Number of times finish timedout")).unwrap();
    static ref NB_QUIC_FINISH_ERRORED: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_finish_errored", "Number of times finish errored")).unwrap();
}

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
        const DATAGRAM_RECEIVE_BUFFER_SIZE: usize = 64 * 1024 * 1024;
        const DATAGRAM_SEND_BUFFER_SIZE: usize = 64 * 1024 * 1024;
        const INITIAL_MAXIMUM_TRANSMISSION_UNIT: u16 = MINIMUM_MAXIMUM_TRANSMISSION_UNIT;
        const MINIMUM_MAXIMUM_TRANSMISSION_UNIT: u16 = 1280;

        let mut endpoint = {
            let client_socket =
                solana_net_utils::bind_in_range(IpAddr::V4(Ipv4Addr::UNSPECIFIED), (8000, 10000))
                    .expect("create_endpoint bind_in_range")
                    .1;
            let mut config = EndpointConfig::default();
            config
                .max_udp_payload_size(MINIMUM_MAXIMUM_TRANSMISSION_UNIT)
                .expect("Should set max MTU");
            quinn::Endpoint::new(config, None, client_socket, Arc::new(TokioRuntime))
                .expect("create_endpoint quinn::Endpoint::new")
        };

        let mut crypto = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification {}))
            .with_client_auth_cert(vec![certificate], key)
            .unwrap();
        crypto.enable_early_data = true;
        crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

        let mut config = ClientConfig::new(Arc::new(crypto));
        let mut transport_config = TransportConfig::default();

        let timeout = IdleTimeout::try_from(Duration::from_secs(1)).unwrap();
        transport_config.max_idle_timeout(Some(timeout));
        transport_config.keep_alive_interval(Some(Duration::from_millis(500)));
        transport_config.datagram_receive_buffer_size(Some(DATAGRAM_RECEIVE_BUFFER_SIZE));
        transport_config.datagram_send_buffer_size(DATAGRAM_SEND_BUFFER_SIZE);
        transport_config.initial_mtu(INITIAL_MAXIMUM_TRANSMISSION_UNIT);
        transport_config.max_concurrent_bidi_streams(VarInt::from(0u8));
        transport_config.max_concurrent_uni_streams(VarInt::from(0u8));
        transport_config.min_mtu(MINIMUM_MAXIMUM_TRANSMISSION_UNIT);
        transport_config.mtu_discovery_config(None);
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
        match timeout(connection_timeout, connecting).await {
            Ok(res) => match res {
                Ok(connection) => Ok(connection),
                Err(e) => {
                    NB_QUIC_CONNECTION_ERRORED.inc();
                    Err(e.into())
                }
            },
            Err(_) => {
                // timed out
                NB_QUIC_CONNECTION_TIMEOUT.inc();
                Err(ConnectionError::TimedOut.into())
            }
        }
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
                    NB_QUIC_0RTT_TIMEOUT.inc();
                    return Err(ConnectionError::TimedOut.into());
                }
            }
            Err(connecting) => {
                if let Ok(connecting_result) = timeout(connection_timeout, connecting).await {
                    if connecting_result.is_err() {
                        NB_QUIC_CONNECTION_ERRORED.inc();
                    }
                    connecting_result?
                } else {
                    NB_QUIC_CONNECTION_TIMEOUT.inc();
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
                    NB_QUIC_WRITEALL_ERRORED.inc();
                    return Err(QuicConnectionError::ConnectionError { retry: true });
                }
            }
            Err(_) => {
                log::debug!("timeout while writing transaction for {}", identity);
                NB_QUIC_WRITEALL_TIMEOUT.inc();
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
                    NB_QUIC_FINISH_ERRORED.inc();
                    return Err(QuicConnectionError::ConnectionError { retry: false });
                }
            }
            Err(_) => {
                log::debug!("timeout while finishing transaction for {}", identity);
                NB_QUIC_FINISH_TIMEOUT.inc();
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
