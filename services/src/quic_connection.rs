use crate::quic_connection_utils::{
    QuicConnectionError, QuicConnectionParameters, QuicConnectionUtils,
};
use futures::FutureExt;
use log::warn;
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use quinn::{Connection, Endpoint};
use solana_lite_rpc_core::structures::rotating_queue::RotatingQueue;
use solana_sdk::pubkey::Pubkey;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

pub type EndpointPool = RotatingQueue<Endpoint>;

lazy_static::lazy_static! {
    static ref NB_QUIC_CONNECTION_RESET: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_nb_connection_reset", "Number of times connection was reset")).unwrap();
    static ref NB_QUIC_CONNECTION_REQUESTED: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_nb_connection_requested", "Number of connections requested")).unwrap();
    static ref TRIED_SEND_TRANSCTION_TRIED: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_nb_send_transaction_tried", "Number of times send transaction was tried")).unwrap();
    static ref SEND_TRANSCTION_SUCESSFUL: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_nb_send_transaction_successful", "Number of times send transaction was successful")).unwrap();
    static ref NB_QUIC_COULDNOT_ESTABLISH_CONNECTION: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_nb_couldnot_establish_connection", "Number of times quic connection could not be established")).unwrap();
}

#[derive(Clone)]
#[warn(clippy::rc_clone_in_vec_init)]
pub struct QuicConnection {
    connection: Arc<RwLock<Option<Connection>>>,
    last_stable_id: Arc<AtomicU64>,
    endpoint: Endpoint,
    identity: Pubkey,
    socket_address: SocketAddr,
    connection_params: QuicConnectionParameters,
    exit_signal: Arc<AtomicBool>,
    timeout_counters: Arc<AtomicU64>,
    has_connected_once: Arc<AtomicBool>,
}

impl QuicConnection {
    pub fn new(
        identity: Pubkey,
        endpoint: Endpoint,
        socket_address: SocketAddr,
        connection_params: QuicConnectionParameters,
        exit_signal: Arc<AtomicBool>,
    ) -> Self {
        Self {
            connection: Arc::new(RwLock::new(None)),
            last_stable_id: Arc::new(AtomicU64::new(0)),
            endpoint,
            identity,
            socket_address,
            connection_params,
            exit_signal,
            timeout_counters: Arc::new(AtomicU64::new(0)),
            has_connected_once: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn connect(&self, is_already_connected: bool) -> Option<Connection> {
        QuicConnectionUtils::connect(
            self.identity,
            is_already_connected,
            self.endpoint.clone(),
            self.socket_address,
            self.connection_params.connection_timeout,
            self.connection_params.connection_retry_count,
            self.exit_signal.clone(),
        )
        .await
    }

    pub async fn get_connection(&self) -> Option<Connection> {
        // get new connection reset if necessary
        let last_stable_id = self.last_stable_id.load(Ordering::Relaxed) as usize;
        let conn = self.connection.read().await.clone();
        match conn {
            Some(connection) => {
                if connection.stable_id() == last_stable_id {
                    let current_stable_id = connection.stable_id();
                    // problematic connection
                    let mut conn = self.connection.write().await;
                    let connection = conn.clone().expect("Connection cannot be None here");
                    // check may be already written by another thread
                    if connection.stable_id() != current_stable_id {
                        Some(connection)
                    } else {
                        NB_QUIC_CONNECTION_RESET.inc();
                        let new_conn = self.connect(true).await;
                        if let Some(new_conn) = new_conn {
                            *conn = Some(new_conn);
                            conn.clone()
                        } else {
                            // could not connect
                            None
                        }
                    }
                } else {
                    Some(connection.clone())
                }
            }
            None => {
                NB_QUIC_CONNECTION_REQUESTED.inc();
                let connection = self.connect(false).await;
                *self.connection.write().await = connection.clone();
                self.has_connected_once.store(true, Ordering::Relaxed);
                connection
            }
        }
    }

    pub async fn send_transaction(&self, tx: Vec<u8>) {
        let connection_retry_count = self.connection_params.connection_retry_count;
        for _ in 0..connection_retry_count {
            if self.exit_signal.load(Ordering::Relaxed) {
                // return
                return;
            }

            let mut do_retry = false;
            let connection = self.get_connection().await;

            if let Some(connection) = connection {
                TRIED_SEND_TRANSCTION_TRIED.inc();
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
                                SEND_TRANSCTION_SUCESSFUL.inc();
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
                    break;
                }
            } else {
                NB_QUIC_COULDNOT_ESTABLISH_CONNECTION.inc();
                warn!(
                    "Could not establish connection with {}",
                    self.identity.to_string()
                );
                break;
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

    pub fn has_connected_atleast_once(&self) -> bool {
        self.has_connected_once.load(Ordering::Relaxed)
    }

    pub async fn is_connected(&self) -> bool {
        let connection = self.connection.read().await.clone();
        match connection {
            Some(connection) => connection.close_reason().is_none(),
            None => false,
        }
    }
}

#[derive(Clone)]
pub struct QuicConnectionPool {
    connections: Vec<QuicConnection>,
    // counting semaphore is ideal way to manage backpressure on the connection
    // because a connection can create only N unistream connections
    transactions_in_sending_semaphore: Vec<Arc<Semaphore>>,
    permit_threshold: usize,
}

pub struct PooledConnection {
    pub connection: QuicConnection,
    pub permit: OwnedSemaphorePermit,
}

impl QuicConnectionPool {
    pub fn new(
        identity: Pubkey,
        endpoints: EndpointPool,
        socket_address: SocketAddr,
        connection_parameters: QuicConnectionParameters,
        exit_signal: Arc<AtomicBool>,
        nb_connection: usize,
        max_number_of_unistream_connection: usize,
    ) -> Self {
        let mut connections = vec![];
        // should not clone connection each time but create a new one
        for _ in 0..nb_connection {
            connections.push(QuicConnection::new(
                identity,
                endpoints.get().expect("Should get and endpoint"),
                socket_address,
                connection_parameters,
                exit_signal.clone(),
            ));
        }
        Self {
            connections,
            transactions_in_sending_semaphore: {
                // should create a new semaphore each time so avoid vec[elem;count]
                let mut v = Vec::with_capacity(nb_connection);
                (0..nb_connection).for_each(|_| {
                    v.push(Arc::new(Semaphore::new(max_number_of_unistream_connection)))
                });
                v
            },
            permit_threshold: max_number_of_unistream_connection
                .saturating_mul(90)
                .saturating_div(100),
        }
    }

    async fn get_permit_and_index(&self) -> anyhow::Result<(OwnedSemaphorePermit, usize)> {
        // pefer getting connection that were already established
        for (index, sem) in self.transactions_in_sending_semaphore.iter().enumerate() {
            let connection = &self.connections[index];

            if !connection.has_connected_atleast_once()
                || (connection.is_connected().await
                    && sem.available_permits() > self.permit_threshold)
            {
                // if it is connection is not yet connected even once or connection is still open
                if let Ok(permit) = sem.clone().try_acquire_owned() {
                    return Ok((permit, index));
                }
            }
        }
        // if all of the connections are full then fall back on semaphore which gets free first.
        let (permit, index, _) = futures::future::select_all(
            self.transactions_in_sending_semaphore
                .iter()
                .map(|x| x.clone().acquire_owned().boxed()),
        )
        .await;
        let permit = permit?;
        Ok((permit, index))
    }

    pub async fn get_pooled_connection(&self) -> anyhow::Result<PooledConnection> {
        let (permit, index) = self.get_permit_and_index().await?;
        // establish a connection if the connection has not yet been used
        let connection = self.connections[index].clone();
        if !connection.has_connected_atleast_once() {
            connection.get_connection().await;
        }
        Ok(PooledConnection { connection, permit })
    }

    pub fn len(&self) -> usize {
        self.connections.len()
    }

    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }
}
