use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use dashmap::DashMap;
use log::error;
use quinn::{
    ClientConfig, Connection, ConnectionError, Endpoint, EndpointConfig, IdleTimeout, TokioRuntime,
    TransportConfig, VarInt,
};
use solana_sdk::pubkey::Pubkey;
use tokio::{
    sync::{broadcast::Receiver, broadcast::Sender},
    time::timeout,
};

use crate::DEFAULT_TX_BATCH_SIZE;

use super::rotating_queue::RotatingQueue;

pub const ALPN_TPU_PROTOCOL_ID: &[u8] = b"solana-tpu";
const QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC: u64 = 5;

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

        let res = timeout(
            Duration::from_secs(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC),
            connecting,
        )
        .await?;
        Ok(res?)
    }

    async fn make_connection_0rtt(
        endpoint: Endpoint,
        addr: SocketAddr,
    ) -> anyhow::Result<Connection> {
        let connecting = endpoint.connect(addr, "connect")?;
        let connection = match connecting.into_0rtt() {
            Ok((connection, zero_rtt)) => {
                if let Ok(_) = timeout(
                    Duration::from_secs(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC),
                    zero_rtt,
                )
                .await
                {
                    connection
                } else {
                    return Err(ConnectionError::TimedOut.into());
                }
            }
            Err(connecting) => {
                if let Ok(connecting_result) = timeout(
                    Duration::from_millis(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC),
                    connecting,
                )
                .await
                {
                    connecting_result?
                } else {
                    return Err(ConnectionError::TimedOut.into());
                }
            }
        };
        Ok(connection)
    }

    async fn listen(
        transaction_reciever: Receiver<Vec<u8>>,
        exit_oneshot_channel: tokio::sync::mpsc::Receiver<()>,
        endpoint: Endpoint,
        addr: SocketAddr,
        exit_signal: Arc<AtomicBool>,
        identity: Pubkey,
    ) {
        let mut already_connected = false;
        let mut connection: Option<(Connection, quinn::SendStream)> = None;
        let mut transaction_reciever = transaction_reciever;
        let mut exit_oneshot_channel = exit_oneshot_channel;
        loop {
            // exit signal set
            if exit_signal.load(Ordering::Relaxed) {
                break;
            }

            tokio::select! {
                tx_or_timeout = timeout(Duration::from_secs(QUIC_CONNECTION_TIMEOUT_DURATION_IN_SEC), transaction_reciever.recv() ) => {
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
                            let (conn, mut send_stream) = match connection {
                                Some(conn) => conn,
                                None => {
                                    let conn = if already_connected {
                                        Self::make_connection(endpoint.clone(), addr.clone()).await
                                    } else {
                                        Self::make_connection_0rtt(endpoint.clone(), addr.clone()).await
                                    };
                                    match conn {
                                        Ok(conn) => {
                                            already_connected = true;
                                            let unistream = conn.open_uni().await;
                                            if let Err(e) = unistream {
                                                error!("error opening a unistream for {} error {}", identity, e);
                                                continue;
                                            }
                                            (conn, unistream.unwrap())
                                        },
                                        Err(e) => {
                                            error!("Could not connect to {} because of error {}", identity, e);
                                            continue;
                                        }
                                    }

                                }
                            };
                            let mut length = 1;
                            let mut tx_batch = tx;
                            while length < DEFAULT_TX_BATCH_SIZE {
                                match transaction_reciever.try_recv() {
                                    Ok(mut tx) => {
                                        length += 1;
                                        tx_batch.append(&mut tx);
                                    },
                                    _ => {
                                        break;
                                    }
                                }
                            }

                            if let Err(e) = send_stream.write_all(tx_batch.as_slice()).await {
                                error!(
                                    "Error while writing transaction for {} error {}",
                                    identity, e
                                );
                            }
                            connection = Some((conn, send_stream));
                        },
                        Err(_) => {
                            // timed out
                            if let Some((_,stream)) = &mut connection {
                                let _ = stream.finish().await;
                                connection = None;
                            }
                        }
                    }
                },
                _ = exit_oneshot_channel.recv() => {
                    if let Some((_,stream)) = &mut connection {
                        let _ = stream.finish().await;
                        connection = None;
                    }

                    break;
                }
            };
        }

        if let Some((_, stream)) = &mut connection {
            let _ = stream.finish().await;
        }
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
    pub exit_channel: tokio::sync::mpsc::Sender<()>,
}

pub struct TpuConnectionManager {
    endpoints: RotatingQueue<Endpoint>,
    identity_to_active_connection: Arc<DashMap<Pubkey, Arc<ActiveConnectionWithExitChannel>>>,
}

impl TpuConnectionManager {
    pub fn new(certificate: rustls::Certificate, key: rustls::PrivateKey, fanout: usize) -> Self {
        Self {
            endpoints: RotatingQueue::new(fanout / 2, || {
                Self::create_endpoint(certificate.clone(), key.clone())
            }),
            identity_to_active_connection: Arc::new(DashMap::new()),
        }
    }

    fn create_endpoint(certificate: rustls::Certificate, key: rustls::PrivateKey) -> Endpoint {
        let mut endpoint = {
            let client_socket =
                solana_net_utils::bind_in_range(IpAddr::V4(Ipv4Addr::UNSPECIFIED), (8000, 60000))
                    .expect("QuicLazyInitializedEndpoint::create_endpoint bind_in_range")
                    .1;
            let config = EndpointConfig::default();
            quinn::Endpoint::new(config, None, client_socket, TokioRuntime)
                .expect("QuicNewConnection::create_endpoint quinn::Endpoint::new")
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

        let timeout = IdleTimeout::from(VarInt::from_u32(10000));
        transport_config.max_idle_timeout(Some(timeout));
        transport_config.keep_alive_interval(Some(Duration::from_millis(1000)));
        config.transport_config(Arc::new(transport_config));

        endpoint.set_default_client_config(config);

        endpoint
    }

    pub async fn update_connections(
        &self,
        transaction_sender: Arc<Sender<Vec<u8>>>,
        connections_to_keep: Vec<(Pubkey, SocketAddr)>,
    ) {
        let mut set_of_identities: HashSet<Pubkey> = HashSet::new();

        for (identity, socket_addr) in connections_to_keep {
            set_of_identities.insert(identity);
            if self.identity_to_active_connection.get(&identity).is_none() {
                let endpoint = self.endpoints.get();
                let active_connection = ActiveConnection::new(endpoint, socket_addr, identity);
                // using mpsc as a oneshot channel/ because with one shot channel we cannot reuse the reciever
                let (sx, rx) = tokio::sync::mpsc::channel(1);

                let transaction_reciever = transaction_sender.subscribe();
                active_connection.start_listening(transaction_reciever, rx);
                self.identity_to_active_connection.insert(
                    identity,
                    Arc::new(ActiveConnectionWithExitChannel {
                        active_connection,
                        exit_channel: sx,
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
            if !set_of_identities.contains(identity) {
                // ignore error for exit channel
                value
                    .active_connection
                    .exit_signal
                    .store(true, Ordering::Relaxed);
                let _ = value.exit_channel.send(()).await;
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
