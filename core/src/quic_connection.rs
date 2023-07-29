use crate::{
    quic_connection_utils::{QuicConnectionError, QuicConnectionParameters, QuicConnectionUtils},
    rotating_queue::RotatingQueue,
};
use quinn::{Connection, Endpoint};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::RwLock;

pub type EndpointPool = RotatingQueue<Endpoint>;

#[derive(Clone)]
pub struct QuicConnection {
    connection: Arc<RwLock<Connection>>,
    last_stable_id: Arc<AtomicU64>,
    endpoint: Endpoint,
    identity: Pubkey,
    socket_address: SocketAddr,
    connection_params: QuicConnectionParameters,
    exit_signal: Arc<AtomicBool>,
    timeout_counters: Arc<AtomicU64>,
}

impl QuicConnection {
    pub async fn new(
        identity: Pubkey,
        endpoints: EndpointPool,
        socket_address: SocketAddr,
        connection_params: QuicConnectionParameters,
        exit_signal: Arc<AtomicBool>,
    ) -> anyhow::Result<Self> {
        let endpoint = endpoints
            .get()
            .await
            .expect("endpoint pool is not suppose to be empty");
        let connection = QuicConnectionUtils::connect(
            identity,
            false,
            endpoint.clone(),
            socket_address,
            connection_params.connection_timeout,
            connection_params.connection_retry_count,
            exit_signal.clone(),
        )
        .await;

        match connection {
            Some(connection) => Ok(Self {
                connection: Arc::new(RwLock::new(connection)),
                last_stable_id: Arc::new(AtomicU64::new(0)),
                endpoint,
                identity,
                socket_address,
                connection_params,
                exit_signal,
                timeout_counters: Arc::new(AtomicU64::new(0)),
            }),
            None => {
                anyhow::bail!("Could not establish connection");
            }
        }
    }

    async fn get_connection(&self) -> Option<Connection> {
        // get new connection reset if necessary
        let last_stable_id = self.last_stable_id.load(Ordering::Relaxed) as usize;
        let conn = self.connection.read().await;
        if conn.stable_id() == last_stable_id {
            let current_stable_id = conn.stable_id();
            // problematic connection
            drop(conn);
            let mut conn = self.connection.write().await;
            // check may be already written by another thread
            if conn.stable_id() != current_stable_id {
                Some(conn.clone())
            } else {
                let new_conn = QuicConnectionUtils::connect(
                    self.identity,
                    true,
                    self.endpoint.clone(),
                    self.socket_address,
                    self.connection_params.connection_timeout,
                    self.connection_params.connection_retry_count,
                    self.exit_signal.clone(),
                )
                .await;
                if let Some(new_conn) = new_conn {
                    *conn = new_conn;
                    Some(conn.clone())
                } else {
                    // could not connect
                    None
                }
            }
        } else {
            Some(conn.clone())
        }
    }

    pub async fn send_transaction_batch(&self, txs: Vec<Vec<u8>>) {
        let mut queue = VecDeque::new();
        for tx in txs {
            queue.push_back(tx);
        }
        let connection_retry_count = self.connection_params.connection_retry_count;
        for _ in 0..connection_retry_count {
            if queue.is_empty() || self.exit_signal.load(Ordering::Relaxed) {
                // return
                return;
            }

            let mut do_retry = false;
            while !queue.is_empty() {
                let tx = queue.pop_front().unwrap();
                let connection = self.get_connection().await;

                if self.exit_signal.load(Ordering::Relaxed) {
                    return;
                }

                if let Some(connection) = connection {
                    let current_stable_id = connection.stable_id() as u64;
                    match QuicConnectionUtils::open_unistream(
                        connection,
                        self.connection_params.unistream_timeout,
                    )
                    .await
                    {
                        Ok(send_stream) => {
                            match QuicConnectionUtils::write_all(
                                send_stream,
                                &tx,
                                self.identity,
                                self.connection_params,
                            )
                            .await
                            {
                                Ok(()) => {
                                    // do nothing
                                }
                                Err(QuicConnectionError::ConnectionError { retry }) => {
                                    do_retry = retry;
                                }
                                Err(QuicConnectionError::TimeOut) => {
                                    self.timeout_counters.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                        Err(QuicConnectionError::ConnectionError { retry }) => {
                            do_retry = retry;
                        }
                        Err(QuicConnectionError::TimeOut) => {
                            self.timeout_counters.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if do_retry {
                        self.last_stable_id
                            .store(current_stable_id, Ordering::Relaxed);
                        queue.push_back(tx);
                        break;
                    }
                } else {
                    log::warn!(
                        "Could not establish connection with {}",
                        self.identity.to_string()
                    );
                    break;
                }
            }
            if !do_retry {
                break;
            }
        }
    }

    pub fn get_timeout_count(&self) -> u64 {
        self.timeout_counters.load(Ordering::Relaxed)
    }

    pub fn reset_timeouts(&self) {
        self.timeout_counters.store(0, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct QuicConnectionPool {
    connections: RotatingQueue<QuicConnection>,
    connection_parameters: QuicConnectionParameters,
    endpoints: EndpointPool,
    identity: Pubkey,
    socket_address: SocketAddr,
    exit_signal: Arc<AtomicBool>,
}

impl QuicConnectionPool {
    pub fn new(
        identity: Pubkey,
        endpoints: EndpointPool,
        socket_address: SocketAddr,
        connection_parameters: QuicConnectionParameters,
        exit_signal: Arc<AtomicBool>,
    ) -> Self {
        let connections = RotatingQueue::new_empty();
        Self {
            connections,
            identity,
            endpoints,
            socket_address,
            connection_parameters,
            exit_signal,
        }
    }

    pub async fn send_transaction_batch(&self, txs: Vec<Vec<u8>>) {
        let connection = match self.connections.get().await {
            Some(connection) => connection,
            None => {
                let new_connection = QuicConnection::new(
                    self.identity,
                    self.endpoints.clone(),
                    self.socket_address,
                    self.connection_parameters,
                    self.exit_signal.clone(),
                )
                .await;
                if new_connection.is_err() {
                    return;
                }
                let new_connection = new_connection.expect("Cannot establish a connection");
                self.connections.add(new_connection.clone()).await;
                new_connection
            }
        };

        connection.send_transaction_batch(txs).await;
    }

    pub async fn add_connection(&self) {
        let new_connection = QuicConnection::new(
            self.identity,
            self.endpoints.clone(),
            self.socket_address,
            self.connection_parameters,
            self.exit_signal.clone(),
        )
        .await;
        if let Ok(new_connection) = new_connection {
            self.connections.add(new_connection).await;
        }
    }

    pub async fn remove_connection(&self) {
        if !self.connections.is_empty() {
            self.connections.remove().await;
        }
    }

    pub fn len(&self) -> usize {
        self.connections.len()
    }

    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }
}
