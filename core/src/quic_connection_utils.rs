use log::{trace, warn};
use quinn::{Connection, ConnectionError, Endpoint, SendStream, EndpointConfig, TokioRuntime, ClientConfig, TransportConfig, IdleTimeout};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::VecDeque,
    net::{SocketAddr, IpAddr, Ipv4Addr},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::RwLock, time::timeout};

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
        crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

        let mut config = ClientConfig::new(Arc::new(crypto));
        let mut transport_config = TransportConfig::default();

        let timeout = IdleTimeout::try_from(Duration::from_secs(1)).unwrap();
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

    pub async fn connect(
        identity: Pubkey,
        already_connected: bool,
        endpoint: Endpoint,
        addr: SocketAddr,
        connection_timeout: Duration,
        connection_retry_count: usize,
        exit_signal: Arc<AtomicBool>,
        on_connect: fn(),
    ) -> Option<Connection> {
        for _ in 0..connection_retry_count {
            let conn = if already_connected {
                Self::make_connection_0rtt(endpoint.clone(), addr, connection_timeout).await
            } else {
                let conn = Self::make_connection(endpoint.clone(), addr, connection_timeout).await;
                conn
            };
            match conn {
                Ok(conn) => {
                    on_connect();
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
        last_stable_id: Arc<AtomicU64>,
        connection_stable_id: u64,
        connection_timeout: Duration,
    ) -> bool {
        let write_timeout_res =
            timeout(connection_timeout, send_stream.write_all(tx.as_slice())).await;
        match write_timeout_res {
            Ok(write_res) => {
                if let Err(e) = write_res {
                    trace!(
                        "Error while writing transaction for {}, error {}",
                        identity,
                        e
                    );
                    // retry
                    last_stable_id.store(connection_stable_id, Ordering::Relaxed);
                    return true;
                }
            }
            Err(_) => {
                warn!("timeout while writing transaction for {}", identity);
            }
        }

        let finish_timeout_res = timeout(connection_timeout, send_stream.finish()).await;
        match finish_timeout_res {
            Ok(finish_res) => {
                if let Err(e) = finish_res {
                    last_stable_id.store(connection_stable_id, Ordering::Relaxed);
                    trace!(
                        "Error while writing transaction for {}, error {}",
                        identity,
                        e
                    );
                    return true;
                }
            }
            Err(_) => {
                warn!("timeout while finishing transaction for {}", identity);
            }
        }

        false
    }

    pub async fn open_unistream(
        connection: Connection,
        last_stable_id: Arc<AtomicU64>,
        connection_timeout: Duration,
    ) -> (Option<SendStream>, bool) {
        match timeout(connection_timeout, connection.open_uni()).await {
            Ok(Ok(unistream)) => (Some(unistream), false),
            Ok(Err(_)) => {
                // reset connection for next retry
                last_stable_id.store(connection.stable_id() as u64, Ordering::Relaxed);
                (None, true)
            }
            Err(_) => (None, false),
        }
    }

    pub async fn send_transaction_batch(
        connection: Arc<RwLock<Connection>>,
        txs: Vec<Vec<u8>>,
        identity: Pubkey,
        endpoint: Endpoint,
        socket_addr: SocketAddr,
        exit_signal: Arc<AtomicBool>,
        last_stable_id: Arc<AtomicU64>,
        connection_timeout: Duration,
        connection_retry_count: usize,
        on_connect: fn(),
    ) {
        let mut queue = VecDeque::new();
        for tx in txs {
            queue.push_back(tx);
        }
        for _ in 0..connection_retry_count {
            if queue.is_empty() || exit_signal.load(Ordering::Relaxed) {
                // return
                return;
            }
            // get new connection reset if necessary
            let conn = {
                let last_stable_id = last_stable_id.load(Ordering::Relaxed) as usize;
                let conn = connection.read().await;
                if conn.stable_id() == last_stable_id {
                    let current_stable_id = conn.stable_id();
                    // problematic connection
                    drop(conn);
                    let mut conn = connection.write().await;
                    // check may be already written by another thread
                    if conn.stable_id() != current_stable_id {
                        conn.clone()
                    } else {
                        let new_conn = Self::connect(
                            identity,
                            true,
                            endpoint.clone(),
                            socket_addr,
                            connection_timeout,
                            connection_retry_count,
                            exit_signal.clone(),
                            on_connect,
                        )
                        .await;
                        if let Some(new_conn) = new_conn {
                            *conn = new_conn;
                            conn.clone()
                        } else {
                            // could not connect
                            return;
                        }
                    }
                } else {
                    conn.clone()
                }
            };
            let mut retry = false;
            while !queue.is_empty() {
                let tx = queue.pop_front().unwrap();
                let (stream, retry_conn) =
                    Self::open_unistream(conn.clone(), last_stable_id.clone(), connection_timeout)
                        .await;
                if let Some(send_stream) = stream {
                    if exit_signal.load(Ordering::Relaxed) {
                        return;
                    }

                    retry = Self::write_all(
                        send_stream,
                        &tx,
                        identity,
                        last_stable_id.clone(),
                        conn.stable_id() as u64,
                        connection_timeout,
                    )
                    .await;
                } else {
                    retry = retry_conn;
                }
                if retry {
                    queue.push_back(tx);
                    break;
                }
            }
            if !retry {
                break;
            }
        }
    }
}

struct SkipServerVerification;

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
