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
use solana_sdk::{
    pubkey::Pubkey,
    quic::{QUIC_MAX_STAKED_RECEIVE_WINDOW_RATIO, QUIC_MIN_STAKED_RECEIVE_WINDOW_RATIO},
};
use solana_streamer::nonblocking::quic::compute_max_allowed_uni_streams;
use tokio::{
    sync::{broadcast::Receiver, broadcast::Sender},
    time::timeout,
};

use super::{rotating_queue::RotatingQueue, tpu_service::IdentityStakes};

pub const ALPN_TPU_PROTOCOL_ID: &[u8] = b"solana-tpu";
const QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC: Duration = Duration::from_secs(5);
const CONNECTION_RETRY_COUNT: usize = 10;

type CopyableOneShotReceiver = tokio::sync::broadcast::Receiver<u8>;
type CopyableOneShotSender = tokio::sync::broadcast::Sender<u8>;

lazy_static::lazy_static! {
    static ref NB_QUIC_CONNECTIONS: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_nb_active_quic_connections", "Number of quic connections open")).unwrap();
    static ref NB_QUIC_TASKS: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_nb_quic_tasks", "Number quic tasks that are running")).unwrap();
    static ref NB_CONNECTIONS_TO_KEEP: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_connections_to_keep", "Number of connections to keep asked by tpu service")).unwrap();
}

// There are two exit mechanisms one is a atomic boolean which should be true if we want to exit
// there is also a broadcast channel where 0 is sent which is used by tokio select to stop waiting a reply from quic
struct ActiveConnection {
    pub endpoint: Endpoint,
    pub identity: Pubkey,
    pub tpu_address: SocketAddr,
    pub exit_flag: Arc<AtomicBool>,
}

impl ActiveConnection {
    pub fn new(endpoint: Endpoint, tpu_address: SocketAddr, identity: Pubkey) -> Self {
        Self {
            endpoint,
            tpu_address,
            identity,
            exit_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn make_connection(
        endpoint: Endpoint,
        addr: SocketAddr,
        mut exit_channel: CopyableOneShotReceiver,
    ) -> anyhow::Result<Connection> {
        let connecting = endpoint.connect(addr, "connect")?;
        tokio::select! {
            res = timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, connecting) => {
                Ok(res??)
            },
            _ = exit_channel.recv() => {
                Err(ConnectionError::TimedOut.into())
            }
        }
    }

    async fn make_connection_0rtt(
        endpoint: Endpoint,
        addr: SocketAddr,
        exit_channel: Arc<CopyableOneShotSender>,
    ) -> anyhow::Result<Connection> {
        let connecting = endpoint.connect(addr, "connect")?;
        let mut exit_recv = exit_channel.subscribe();

        match connecting.into_0rtt() {
            Ok((connection, zero_rtt)) => {
                tokio::select! {
                    _ = timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, zero_rtt) => {
                        Ok(connection)
                    },
                    _ = exit_recv.recv() => {
                        Err(ConnectionError::TimedOut.into())
                    }
                }
            }
            Err(connecting) => {
                tokio::select! {
                    res = timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, connecting) => {
                        let res = res??;
                        Ok(res)
                    },
                    _ = exit_recv.recv() => {
                        Err(ConnectionError::TimedOut.into())
                    }
                }
            }
        }
    }

    async fn connect(
        identity: Pubkey,
        already_connected: bool,
        endpoint: Endpoint,
        addr: SocketAddr,
        exit_signal: Arc<AtomicBool>,
        exit_channel: Arc<CopyableOneShotSender>,
    ) -> Option<Arc<Connection>> {
        for _i in 0..CONNECTION_RETRY_COUNT {
            if exit_signal.load(Ordering::Relaxed) {
                break;
            }

            let conn = if already_connected {
                info!("making make_connection_0rtt");
                Self::make_connection_0rtt(endpoint.clone(), addr.clone(), exit_channel.clone())
                    .await
            } else {
                info!("making make_connection");
                Self::make_connection(endpoint.clone(), addr.clone(), exit_channel.subscribe())
                    .await
            };
            match conn {
                Ok(conn) => {
                    NB_QUIC_CONNECTIONS.inc();
                    return Some(Arc::new(conn));
                }
                Err(e) => {
                    trace!("Could not connect to {} because of error {}", identity, e);
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
        exit_channel_sender: Arc<CopyableOneShotSender>,
    ) -> Option<SendStream> {
        let (unistream, reconnect_and_try_again) = match connection {
            Some(conn) => {
                if exit_signal.load(Ordering::Relaxed) {
                    return None;
                }
                let mut exit_reciever = exit_channel_sender.subscribe();

                tokio::select! {
                    unistream_maybe_timeout = timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, conn.open_uni()) => {
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
                    },
                    _ = exit_reciever.recv() => {
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
                exit_channel_sender.clone(),
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
                        exit_channel_sender.clone(),
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

    async fn write_all(
        mut send_stream: SendStream,
        tx: Vec<u8>,
        identity: Pubkey,
        mut exit_signal: CopyableOneShotReceiver,
    ) {
        tokio::select! {
             write_timeout_res = timeout( QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, send_stream.write_all(tx.as_slice())) => {
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
            },
            _ = exit_signal.recv() => {
            }
        };

        let finish_timeout_res = timeout(
            QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC,
            send_stream.finish(),
        )
        .await;
        match finish_timeout_res {
            Ok(finish_res) => {
                if let Err(e) = finish_res {
                    warn!(
                        "Error while writing transaction for {}, error {}",
                        identity, e
                    );
                }
            }
            Err(_) => {
                warn!("timeout while writing transaction for {}", identity);
            }
        }
    }

    async fn listen(
        transaction_reciever: Receiver<Vec<u8>>,
        exit_oneshot_channel_sender: Arc<CopyableOneShotSender>,
        endpoint: Endpoint,
        addr: SocketAddr,
        exit_flag: Arc<AtomicBool>,
        identity: Pubkey,
        identity_stakes: IdentityStakes,
    ) {
        NB_QUIC_TASKS.inc();
        let mut already_connected = false;
        let mut connection: Option<Arc<Connection>> = None;
        let mut transaction_reciever = transaction_reciever;
        let mut exit_oneshot_channel = exit_oneshot_channel_sender.subscribe();
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
        let send_limit = (max_uni_stream_connections * number_of_transactions_per_unistream - 1)
            .max(1)
            .min(2048 * 10 - 1);

        loop {
            // exit signal set
            if exit_flag.load(Ordering::Relaxed) {
                break;
            }
            tokio::select! {
                tx_or_timeout = timeout(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC, transaction_reciever.recv() ) => {
                    match tx_or_timeout {
                        Ok(first_tx) => {
                            let first_tx: Vec<u8> = match first_tx {
                                Ok(tx) => tx,
                                Err(e) => {
                                    // client is lagging
                                    error!(
                                        "Broadcast channel error on recv for {} error {}",
                                        identity, e
                                    );
                                    continue;
                                }
                            };
                            let mut txs = vec![first_tx];
                            let mut tx_count = 1;
                            // retrieve as many transactions as possible with the limit
                            while tx_count < send_limit {
                                if let Ok(tx) = transaction_reciever.try_recv() {
                                    txs.push(tx);
                                    tx_count += 1;
                                } else {
                                    break;
                                }
                            }

                            let mut batches = txs.chunks(number_of_transactions_per_unistream as usize).map(|txs| {
                                let mut ret_vec = vec![];
                                for tx in txs {
                                    ret_vec.append(&mut tx.clone());
                                }
                                ret_vec
                            }).collect::<Vec<_>>();

                            // save the memory / as all the txs are copied into batches
                            txs.clear();

                            let unistream = Self::open_unistream(
                                &mut connection,
                                true,
                                identity.clone(),
                                already_connected,
                                endpoint.clone(),
                                addr.clone(),
                                exit_flag.clone(),
                                exit_oneshot_channel_sender.clone(),
                            ).await;

                            if !already_connected && connection.is_some() {
                                already_connected = true;
                            }

                            match unistream {
                                Some(send_stream) => {

                                    // we have a fresh connection with a new unistream
                                    // we use the created unistream for first batch and then
                                    // create other unistream for other batches
                                    let first_batch = batches.pop().unwrap();

                                    let first_task = {
                                        let identity = identity.clone();
                                        let exit_signal = exit_oneshot_channel_sender.subscribe();
                                        tokio::spawn(async move {
                                            Self::write_all(send_stream, first_batch, identity, exit_signal).await
                                        })
                                    };
                                    let mut tasks = vec![first_task];
                                    // create multiple parallel streams to the server to send transactions
                                    for batch in batches {
                                        let identity = identity.clone();
                                        let exit_signal = exit_oneshot_channel_sender.subscribe();
                                        let mut exit_signal_2 = exit_oneshot_channel_sender.subscribe();
                                        let connection = connection.clone().unwrap();
                                        let new_task = tokio::spawn(async move {
                                            tokio::select!{
                                                send_stream = connection.open_uni() => {
                                                    if let Ok(send_stream) = send_stream {
                                                        Self::write_all(send_stream, batch, identity, exit_signal).await
                                                    } else {
                                                        // error opening a unistream connection
                                                        warn!("error opening the unistream connection with {}", identity);
                                                    }
                                            },
                                            _ = exit_signal_2.recv() => {
                                                // exit signal got return
                                            }
                                        }
                                        });
                                        tasks.push(new_task);
                                    }
                                    futures::future::join_all(tasks).await;
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
                            continue;
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
        exit_oneshot_channel: Arc<CopyableOneShotSender>,
        identity_stakes: IdentityStakes,
    ) {
        let endpoint = self.endpoint.clone();
        let addr = self.tpu_address.clone();
        let exit_flag = self.exit_flag.clone();
        let identity = self.identity.clone();
        tokio::spawn(async move {
            Self::listen(
                transaction_reciever,
                exit_oneshot_channel,
                endpoint,
                addr,
                exit_flag,
                identity,
                identity_stakes,
            )
            .await;
        });
    }
}

struct ActiveConnectionWithExitChannel {
    pub active_connection: ActiveConnection,
    pub exit_stream: Arc<CopyableOneShotSender>,
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
            if self.identity_to_active_connection.get(&identity).is_none() {
                trace!("added a connection for {}, {}", identity, socket_addr);
                let endpoint = self.endpoints.get();
                let active_connection =
                    ActiveConnection::new(endpoint, socket_addr.clone(), identity.clone());
                // using mpsc as a oneshot channel/ because with one shot channel we cannot reuse the reciever
                let (sx, _) = tokio::sync::broadcast::channel(1);

                let transaction_reciever = transaction_sender.subscribe();
                let sx = Arc::new(sx);
                active_connection.start_listening(
                    transaction_reciever,
                    sx.clone(),
                    identity_stakes,
                );
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
                    .exit_flag
                    .store(true, Ordering::Relaxed);
                let _ = value.exit_stream.send(0);
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
