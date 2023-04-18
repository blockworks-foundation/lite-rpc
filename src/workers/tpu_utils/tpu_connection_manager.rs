use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use dashmap::DashMap;
use log::{error, trace, warn};
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use quinn::{
    ClientConfig, Connection, ConnectionError, Endpoint, EndpointConfig, IdleTimeout, SendStream,
    TokioRuntime, TransportConfig,
};
use solana_sdk::{
    pubkey::Pubkey,
    quic::{QUIC_MAX_STAKED_RECEIVE_WINDOW_RATIO, QUIC_MIN_STAKED_RECEIVE_WINDOW_RATIO},
};
use solana_streamer::nonblocking::quic::compute_max_allowed_uni_streams;
use tokio::{
    sync::{broadcast::Receiver, broadcast::Sender, RwLock},
    time::timeout,
};

use super::{rotating_queue::RotatingQueue, tpu_service::IdentityStakes};

pub const ALPN_TPU_PROTOCOL_ID: &[u8] = b"solana-tpu";
const QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC: Duration = Duration::from_secs(10);
const CONNECTION_RETRY_COUNT: usize = 10;

lazy_static::lazy_static! {
    static ref NB_QUIC_CONNECTIONS: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_nb_active_quic_connections", "Number of quic connections open")).unwrap();
    static ref NB_QUIC_ACTIVE_CONNECTIONS: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_nb_active_connections", "Number quic tasks that are running")).unwrap();
    static ref NB_CONNECTIONS_TO_KEEP: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_connections_to_keep", "Number of connections to keep asked by tpu service")).unwrap();
    static ref NB_QUIC_TASKS: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_tasks", "Number of connections to keep asked by tpu service")).unwrap();
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
        let res = timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, connecting).await??;
        Ok(res)
    }

    async fn make_connection_0rtt(
        endpoint: Endpoint,
        addr: SocketAddr,
    ) -> anyhow::Result<Connection> {
        let connecting = endpoint.connect(addr, "connect")?;
        let connection = match connecting.into_0rtt() {
            Ok((connection, zero_rtt)) => {
                if (timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, zero_rtt).await).is_ok() {
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
    ) -> Option<Connection> {
        for _i in 0..CONNECTION_RETRY_COUNT {
            let conn = if already_connected {
                Self::make_connection_0rtt(endpoint.clone(), addr).await
            } else {
                let conn = Self::make_connection(endpoint.clone(), addr).await;
                conn
            };
            match conn {
                Ok(conn) => {
                    NB_QUIC_CONNECTIONS.inc();
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

    async fn write_all(
        mut send_stream: SendStream,
        tx: &Vec<u8>,
        identity: Pubkey,
        last_stable_id: Arc<AtomicU64>,
        connection_stable_id: u64,
    ) -> bool {
        let write_timeout_res = timeout(
            QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC,
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
                    // retry
                    last_stable_id.store(connection_stable_id, Ordering::Relaxed);
                    return true;
                }
            }
            Err(_) => {
                warn!("timeout while writing transaction for {}", identity);
            }
        }

        let finish_timeout_res = timeout(
            QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC,
            send_stream.finish(),
        )
        .await;
        match finish_timeout_res {
            Ok(finish_res) => {
                if let Err(e) = finish_res {
                    last_stable_id.store(connection_stable_id, Ordering::Relaxed);
                    trace!(
                        "Error while writing transaction for {}, error {}",
                        identity,
                        e
                    );
                }
            }
            Err(_) => {
                warn!("timeout while writing transaction for {}", identity);
            }
        }

        false
    }

    async fn open_unistream(
        connection: Connection,
        last_stable_id: Arc<AtomicU64>,
    ) -> (Option<SendStream>, bool) {
        match timeout(
            QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC,
            connection.open_uni(),
        )
        .await
        {
            Ok(Ok(unistream)) => return (Some(unistream), false),
            Ok(Err(_)) => {
                // reset connection for next retry
                last_stable_id.store(connection.stable_id() as u64, Ordering::Relaxed);
                (None, true)
            }
            Err(_) => return (None, false),
        }
    }

    async fn send_transaction_batch(
        connection: Arc<RwLock<Connection>>,
        txs: Vec<Vec<u8>>,
        identity: Pubkey,
        endpoint: Endpoint,
        socket_addr: SocketAddr,
        exit_signal: Arc<AtomicBool>,
        last_stable_id: Arc<AtomicU64>,
    ) {
        for _ in 0..3 {
            if exit_signal.load(Ordering::Relaxed) {
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
                            socket_addr.clone(),
                            exit_signal.clone(),
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
            for tx in &txs {
                let (stream, retry_conn) =
                    Self::open_unistream(conn.clone(), last_stable_id.clone()).await;
                if let Some(send_stream) = stream {
                    if exit_signal.load(Ordering::Relaxed) {
                        return;
                    }

                    retry = Self::write_all(
                        send_stream,
                        tx,
                        identity,
                        last_stable_id.clone(),
                        conn.stable_id() as u64,
                    )
                    .await;
                } else {
                    retry = retry_conn;
                }
            }
            if !retry {
                break;
            }
        }
    }

    // copied from solana code base
    fn compute_receive_window_ratio_for_staked_node(
        max_stake: u64,
        min_stake: u64,
        stake: u64,
    ) -> u64 {
        if stake > max_stake {
            return QUIC_MAX_STAKED_RECEIVE_WINDOW_RATIO;
        }

        let max_ratio = QUIC_MAX_STAKED_RECEIVE_WINDOW_RATIO;
        let min_ratio = QUIC_MIN_STAKED_RECEIVE_WINDOW_RATIO;
        if max_stake > min_stake {
            let a = (max_ratio - min_ratio) as f64 / (max_stake - min_stake) as f64;
            let b = max_ratio as f64 - ((max_stake as f64) * a);
            let ratio = (a * stake as f64) + b;
            ratio.round() as u64
        } else {
            QUIC_MAX_STAKED_RECEIVE_WINDOW_RATIO
        }
    }

    async fn listen(
        transaction_reciever: Receiver<Vec<u8>>,
        exit_oneshot_channel: tokio::sync::mpsc::Receiver<()>,
        endpoint: Endpoint,
        addr: SocketAddr,
        exit_signal: Arc<AtomicBool>,
        identity: Pubkey,
        identity_stakes: IdentityStakes,
    ) {
        NB_QUIC_ACTIVE_CONNECTIONS.inc();
        let mut transaction_reciever = transaction_reciever;
        let mut exit_oneshot_channel = exit_oneshot_channel;

        let max_uni_stream_connections: u64 = compute_max_allowed_uni_streams(
            identity_stakes.peer_type,
            identity_stakes.stakes,
            identity_stakes.total_stakes,
        ) as u64;
        let number_of_transactions_per_unistream = match identity_stakes.peer_type {
            solana_streamer::nonblocking::quic::ConnectionPeerType::Staked => {
                Self::compute_receive_window_ratio_for_staked_node(
                    identity_stakes.max_stakes,
                    identity_stakes.min_stakes,
                    identity_stakes.stakes,
                )
            }
            solana_streamer::nonblocking::quic::ConnectionPeerType::Unstaked => 1,
        };

        let task_counter: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
        let mut connection: Option<Arc<RwLock<Connection>>> = None;
        let last_stable_id: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

        loop {
            // exit signal set
            if exit_signal.load(Ordering::Relaxed) {
                break;
            }

            if task_counter.load(Ordering::Relaxed) > max_uni_stream_connections {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                continue;
            }

            tokio::select! {
                tx_or_timeout = timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, transaction_reciever.recv() ) => {
                    // exit signal set
                    if exit_signal.load(Ordering::Relaxed) {
                        break;
                    }

                    match tx_or_timeout {
                        Ok(tx) => {
                            let first_tx: Vec<u8> = match tx {
                                Ok(tx) => tx,
                                Err(e) => {
                                    error!(
                                        "Broadcast channel error on recv for {} error {}",
                                        identity, e
                                    );
                                    continue;
                                }
                            };

                            let mut txs = vec![first_tx];
                            for _ in 1..number_of_transactions_per_unistream {
                                if let Ok(tx) = transaction_reciever.try_recv() {
                                    txs.push(tx);
                                }
                            }

                            if connection.is_none() {
                                // initial connection
                                let conn = Self::connect(identity, false, endpoint.clone(), addr.clone(), exit_signal.clone()).await;
                                if let Some(conn) = conn {
                                    // could connect
                                    connection = Some(Arc::new(RwLock::new(conn)));
                                } else {
                                    break;
                                }
                            }

                            let task_counter = task_counter.clone();
                            let endpoint = endpoint.clone();
                            let exit_signal = exit_signal.clone();
                            let addr = addr.clone();
                            let connection = connection.clone();
                            let last_stable_id = last_stable_id.clone();

                            tokio::spawn(async move {
                                task_counter.fetch_add(1, Ordering::Relaxed);
                                NB_QUIC_TASKS.inc();
                                let connection = connection.unwrap();
                                Self::send_transaction_batch(connection, txs, identity, endpoint, addr, exit_signal, last_stable_id).await;

                                NB_QUIC_TASKS.dec();
                                task_counter.fetch_sub(1, Ordering::Relaxed);
                            });
                            tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
                        },
                        Err(_) => {
                            // timed out
                        }
                    }
                },
                _ = exit_oneshot_channel.recv() => {
                    break;
                }
            };
        }
        drop(transaction_reciever);
        NB_QUIC_ACTIVE_CONNECTIONS.dec();
    }

    pub fn start_listening(
        &self,
        transaction_reciever: Receiver<Vec<u8>>,
        exit_oneshot_channel: tokio::sync::mpsc::Receiver<()>,
        identity_stakes: IdentityStakes,
    ) {
        let endpoint = self.endpoint.clone();
        let addr = self.tpu_address;
        let exit_signal = self.exit_signal.clone();
        let identity = self.identity;
        tokio::spawn(async move {
            Self::listen(
                transaction_reciever,
                exit_oneshot_channel,
                endpoint,
                addr,
                exit_signal,
                identity,
                identity_stakes,
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
        identity_stakes: IdentityStakes,
    ) {
        NB_CONNECTIONS_TO_KEEP.set(connections_to_keep.len() as i64);
        for (identity, socket_addr) in &connections_to_keep {
            if self.identity_to_active_connection.get(identity).is_none() {
                trace!("added a connection for {}, {}", identity, socket_addr);
                let endpoint = self.endpoints.get();
                let active_connection = ActiveConnection::new(endpoint, *socket_addr, *identity);
                // using mpsc as a oneshot channel/ because with one shot channel we cannot reuse the reciever
                let (sx, rx) = tokio::sync::mpsc::channel(1);

                let transaction_reciever = transaction_sender.subscribe();
                active_connection.start_listening(transaction_reciever, rx, identity_stakes);
                self.identity_to_active_connection.insert(
                    *identity,
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
            .map(|x| (*x.key(), x.value().clone()))
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
