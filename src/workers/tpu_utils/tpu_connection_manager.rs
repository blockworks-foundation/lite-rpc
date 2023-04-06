use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use async_recursion::async_recursion;
use dashmap::DashMap;
use log::{error, info, trace, warn};
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use quinn::{
    ClientConfig, Connection, ConnectionError, Endpoint, EndpointConfig, IdleTimeout, SendStream,
    TokioRuntime, TransportConfig,
};
use solana_sdk::pubkey::Pubkey;
use tokio::{
    sync::{broadcast::Receiver, broadcast::Sender},
    time::timeout,
};

use super::rotating_queue::RotatingQueue;

pub const ALPN_TPU_PROTOCOL_ID: &[u8] = b"solana-tpu";
const QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC: Duration = Duration::from_secs(10);
const CONNECTION_RETRY_COUNT: usize = 10;

lazy_static::lazy_static! {
    static ref NB_QUIC_CONNECTIONS: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_nb_active_quic_connections", "Number of quic connections open")).unwrap();
    static ref NB_QUIC_TASKS: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_nb_quic_tasks", "Number quic tasks that are running")).unwrap();
    static ref NB_CONNECTIONS_TO_KEEP: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_connections_to_keep", "Number of connections to keep asked by tpu service")).unwrap();
}

struct ActiveConnection {
    pub endpoint: Endpoint,
    pub identity: Pubkey,
    pub tpu_address: SocketAddr,
    pub exit_signal: Arc<AtomicBool>,
}

impl ActiveConnection {
    pub fn new(endpoint: Endpoint, tpu_address: SocketAddr, identity: Pubkey) -> Self {
        Self {
            endpoint,
            tpu_address,
            identity,
            exit_signal: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn make_connection(endpoint: Endpoint, addr: SocketAddr) -> anyhow::Result<Connection> {
        let connecting = endpoint.connect(addr, "connect")?;
        let res = timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, connecting).await?;
        Ok(res.unwrap())
    }

    async fn make_connection_0rtt(
        endpoint: Endpoint,
        addr: SocketAddr,
    ) -> anyhow::Result<Connection> {
        let connecting = endpoint.connect(addr, "connect")?;
        let connection = match connecting.into_0rtt() {
            Ok((connection, zero_rtt)) => {
                if let Ok(_) = timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, zero_rtt).await {
                    connection
                } else {
                    return Err(ConnectionError::TimedOut.into());
                }
            }
            Err(connecting) => {
                if let Ok(connecting_result) =
                    timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, connecting).await
                {
                    connecting_result?
                } else {
                    return Err(ConnectionError::TimedOut.into());
                }
            }
        };
        Ok(connection)
    }

    async fn connect(
        identity: Pubkey,
        already_connected: bool,
        endpoint: Endpoint,
        addr: SocketAddr,
        exit_signal: Arc<AtomicBool>,
    ) -> Option<Arc<Connection>> {
        for _i in 0..CONNECTION_RETRY_COUNT {
            let conn = if already_connected {
                info!("making make_connection_0rtt");
                Self::make_connection_0rtt(endpoint.clone(), addr.clone()).await
            } else {
                info!("making make_connection");
                Self::make_connection(endpoint.clone(), addr.clone()).await
            };
            match conn {
                Ok(conn) => {
                    NB_QUIC_CONNECTIONS.inc();
                    return Some(Arc::new(conn));
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

    #[async_recursion]
    async fn open_unistream(
        connection: &mut Option<Arc<Connection>>,
        reconnect: bool,
        identity: Pubkey,
        already_connected: bool,
        endpoint: Endpoint,
        addr: SocketAddr,
        exit_signal: Arc<AtomicBool>,
    ) -> Option<SendStream> {
        let (unistream, reconnect_and_try_again) = match connection {
            Some(conn) => {
                let unistream_maybe_timeout =
                    timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, conn.open_uni()).await;
                match unistream_maybe_timeout {
                    Ok(unistream_res) => match unistream_res {
                        Ok(unistream) => (Some(unistream), false),
                        Err(_) => (None, reconnect),
                    },
                    Err(_) => {
                        // timed out
                        (None, false)
                    }
                }
            }
            None => (None, true),
        };

        if reconnect_and_try_again {
            let conn = Self::connect(
                identity,
                already_connected,
                endpoint.clone(),
                addr.clone(),
                exit_signal.clone(),
            )
            .await;
            match conn {
                Some(conn) => {
                    *connection = Some(conn);
                    Self::open_unistream(
                        connection,
                        false,
                        identity,
                        already_connected,
                        endpoint,
                        addr,
                        exit_signal,
                    )
                    .await
                }
                None => {
                    // connection with the peer is not possible
                    None
                }
            }
        } else {
            unistream
        }
    }

    async fn listen(
        transaction_reciever: Receiver<Vec<u8>>,
        exit_oneshot_channel: tokio::sync::mpsc::Receiver<()>,
        endpoint: Endpoint,
        addr: SocketAddr,
        exit_signal: Arc<AtomicBool>,
        identity: Pubkey,
    ) {
        NB_QUIC_TASKS.inc();
        let mut already_connected = false;
        let mut connection: Option<Arc<Connection>> = None;
        let mut transaction_reciever = transaction_reciever;
        let mut exit_oneshot_channel = exit_oneshot_channel;

        loop {
            // exit signal set
            if exit_signal.load(Ordering::Relaxed) {
                break;
            }

            tokio::select! {
                tx_or_timeout = timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, transaction_reciever.recv() ) => {
                    // exit signal set
                    if exit_signal.load(Ordering::Relaxed) {
                        break;
                    }

                    match tx_or_timeout {
                        Ok(tx) => {
                            let tx: Vec<u8> = match tx {
                                Ok(tx) => tx,
                                Err(e) => {
                                    error!(
                                        "Broadcast channel error on recv for {} error {}",
                                        identity, e
                                    );
                                    continue;
                                }
                            };
                            let unistream = Self::open_unistream(
                                &mut connection,
                                true,
                                identity.clone(),
                                already_connected,
                                endpoint.clone(),
                                addr.clone(),
                                exit_signal.clone(),
                            ).await;

                            if !already_connected && connection.is_some() {
                                already_connected = true;
                            }

                            match unistream {
                                Some(mut send_stream) => {
                                    trace!("Sending {} transaction", identity);
                                    let write_timeout_res = timeout( QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, send_stream.write_all(tx.as_slice())).await;
                                    match write_timeout_res {
                                        Ok(write_res) => {
                                            if let Err(e) = write_res {
                                                warn!(
                                                    "Error while writing transaction for {}, error {}",
                                                    identity,
                                                    e
                                                );
                                            }
                                        },
                                        Err(_) => {
                                            warn!(
                                                "timeout while writing transaction for {}",
                                                identity
                                            );
                                        }
                                    }

                                    let finish_timeout_res = timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, send_stream.finish()).await;
                                    match finish_timeout_res {
                                        Ok(finish_res) => {
                                            if let Err(e) = finish_res {
                                                warn!(
                                                    "Error while writing transaction for {}, error {}",
                                                    identity,
                                                    e
                                                );
                                            }
                                        },
                                        Err(_) => {
                                            warn!(
                                                "timeout while writing transaction for {}",
                                                identity
                                            );
                                        }
                                    }
                                },
                                None => {
                                    trace!("could not create a unistream for {}", identity);
                                    break;
                                }
                            }
                        },
                        Err(_) => {
                            // timed out
                            if let Some(_) = &mut connection {
                                NB_QUIC_CONNECTIONS.dec();
                                connection = None;
                            }
                        }
                    }
                },
                _ = exit_oneshot_channel.recv() => {
                    break;
                }
            };
        }

        if let Some(_) = &mut connection {
            NB_QUIC_CONNECTIONS.dec();
        }
        NB_QUIC_TASKS.dec();
    }

    pub fn start_listening(
        &self,
        transaction_reciever: Receiver<Vec<u8>>,
        exit_oneshot_channel: tokio::sync::mpsc::Receiver<()>,
    ) {
        let endpoint = self.endpoint.clone();
        let addr = self.tpu_address.clone();
        let exit_signal = self.exit_signal.clone();
        let identity = self.identity.clone();
        tokio::spawn(async move {
            Self::listen(
                transaction_reciever,
                exit_oneshot_channel,
                endpoint,
                addr,
                exit_signal,
                identity,
            )
            .await;
        });
    }
}

struct ActiveConnectionWithExitChannel {
    pub active_connection: ActiveConnection,
    pub exit_stream: tokio::sync::mpsc::Sender<()>,
}

pub struct TpuConnectionManager {
    endpoints: RotatingQueue<Endpoint>,
    identity_to_active_connection: Arc<DashMap<Pubkey, Arc<ActiveConnectionWithExitChannel>>>,
}

impl TpuConnectionManager {
    pub fn new(certificate: rustls::Certificate, key: rustls::PrivateKey, fanout: usize) -> Self {
        let number_of_clients = if fanout > 5 { fanout / 4 } else { 1 };
        Self {
            endpoints: RotatingQueue::new(number_of_clients, || {
                Self::create_endpoint(certificate.clone(), key.clone())
            }),
            identity_to_active_connection: Arc::new(DashMap::new()),
        }
    }

    fn create_endpoint(certificate: rustls::Certificate, key: rustls::PrivateKey) -> Endpoint {
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

    pub async fn update_connections(
        &self,
        transaction_sender: Arc<Sender<Vec<u8>>>,
        connections_to_keep: HashMap<Pubkey, SocketAddr>,
    ) {
        NB_CONNECTIONS_TO_KEEP.set(connections_to_keep.len() as i64);
        for (identity, socket_addr) in &connections_to_keep {
            if self.identity_to_active_connection.get(&identity).is_none() {
                trace!("added a connection for {}, {}", identity, socket_addr);
                let endpoint = self.endpoints.get();
                let active_connection =
                    ActiveConnection::new(endpoint, socket_addr.clone(), identity.clone());
                // using mpsc as a oneshot channel/ because with one shot channel we cannot reuse the reciever
                let (sx, rx) = tokio::sync::mpsc::channel(1);

                let transaction_reciever = transaction_sender.subscribe();
                active_connection.start_listening(transaction_reciever, rx);
                self.identity_to_active_connection.insert(
                    identity.clone(),
                    Arc::new(ActiveConnectionWithExitChannel {
                        active_connection,
                        exit_stream: sx,
                    }),
                );
            }
        }

        // remove connections which are no longer needed
        let collect_current_active_connections = self
            .identity_to_active_connection
            .iter()
            .map(|x| (x.key().clone(), x.value().clone()))
            .collect::<Vec<_>>();
        for (identity, value) in collect_current_active_connections.iter() {
            if !connections_to_keep.contains_key(identity) {
                trace!("removing a connection for {}", identity);
                // ignore error for exit channel
                value
                    .active_connection
                    .exit_signal
                    .store(true, Ordering::Relaxed);
                let _ = value.exit_stream.send(()).await;
                self.identity_to_active_connection.remove(identity);
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
