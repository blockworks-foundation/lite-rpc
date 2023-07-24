use log::{error, info, trace, warn};
use quinn::{ClientConfig, Connection, ConnectionError, Endpoint, EndpointConfig, IdleTimeout, SendStream, TokioRuntime, TransportConfig, WriteError};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::VecDeque,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use anyhow::bail;
use tokio::{sync::RwLock, time::timeout};
use tokio::time::error::Elapsed;

const ALPN_TPU_PROTOCOL_ID: &[u8] = b"solana-tpu";

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
        // FIXME TEMP HACK TO ALLOW PROXY PROTOCOL
        const ALPN_TPU_FORWARDPROXY_PROTOCOL_ID: &[u8] = b"solana-tpu-forward-proxy";

        crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec(), ALPN_TPU_FORWARDPROXY_PROTOCOL_ID.to_vec()];

        let mut config = ClientConfig::new(Arc::new(crypto));
        let mut transport_config = TransportConfig::default();

        // TODO check timing
        let timeout = IdleTimeout::try_from(Duration::from_secs(5)).unwrap();
        transport_config.max_idle_timeout(Some(timeout));
        transport_config.keep_alive_interval(Some(Duration::from_millis(500)));
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
        tpu_address: SocketAddr,
        connection_timeout: Duration,
        connection_retry_count: usize,
        exit_signal: Arc<AtomicBool>,
        on_connect: fn(),
    ) -> Option<Connection> {
        for _ in 0..connection_retry_count {
            let conn = if already_connected {
                Self::make_connection_0rtt(endpoint.clone(), tpu_address, connection_timeout).await
            } else {
                Self::make_connection(endpoint.clone(), tpu_address, connection_timeout).await
            };
            match conn {
                Ok(conn) => {
                    on_connect();
                    return Some(conn);
                }
                Err(e) => {
                    warn!("Could not connect to tpu {}/{}, error: {}", tpu_address, identity, e);
                    if exit_signal.load(Ordering::Relaxed) {
                        break;
                    }
                }
            }
        }
        None
    }

    pub async fn write_all(
        send_stream: &mut SendStream,
        tx: &Vec<u8>,
        connection_timeout: Duration,
    )  {
        let write_timeout_res =
            timeout(connection_timeout, send_stream.write_all(tx.as_slice())).await;
        match write_timeout_res {
            Ok(write_res) => {
                if let Err(e) = write_res {
                    trace!(
                        "Error while writing transaction for TBD, error {}",
                        // identity, // TODO add more context
                        e
                    );
                    return;
                }
            }
            Err(_) => {
                warn!("timeout while writing transaction for TBD"); // TODO add more context
                panic!("TODO handle timeout"); // FIXME
            }
        }

        let finish_timeout_res = timeout(connection_timeout, send_stream.finish()).await;
        match finish_timeout_res {
            Ok(finish_res) => {
                if let Err(e) = finish_res {
                    // last_stable_id.store(connection_stable_id, Ordering::Relaxed);
                    trace!(
                        "Error while writing transaction for TBD, error {}",
                        // identity,
                        e
                    );
                    return;
                }
            }
            Err(_) => {
                warn!("timeout while finishing transaction for TBD"); // TODO
                panic!("TODO handle timeout"); // FIXME
            }
        }

    }

    pub async fn open_unistream(
        connection: Connection,
        connection_timeout: Duration,
    ) -> (Option<SendStream>, bool) {
        match timeout(connection_timeout, connection.open_uni()).await {
            Ok(Ok(unistream)) => (Some(unistream), false),
            Ok(Err(_)) => {
                // reset connection for next retry
                (None, true)
            }
            // timeout
            Err(_) => (None, false),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn send_transaction_batch(
        connection: Connection,
        txs: Vec<Vec<u8>>,
        exit_signal: Arc<AtomicBool>,
        connection_timeout: Duration,
    ) {
        let (mut stream, _retry_conn) =
            Self::open_unistream(connection.clone(), connection_timeout)
                .await;
        if let Some(ref mut send_stream) = stream {
            if exit_signal.load(Ordering::Relaxed) {
                return;
            }

            for tx in txs {
                let write_timeout_res =
                    timeout(connection_timeout, send_stream.write_all(tx.as_slice())).await;
                match write_timeout_res {
                    Ok(no_timeout) => {
                        match no_timeout {
                            Ok(()) => {}
                            Err(write_error) => {
                                error!("Error writing transaction to stream: {}", write_error);
                            }
                        }
                    }
                    Err(elapsed) => {
                        warn!("timeout sending transactions")
                    }
                }
            }
            // TODO wrap in timeout
            stream.unwrap().finish().await.unwrap();

        } else {
            panic!("no retry handling"); // FIXME
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
