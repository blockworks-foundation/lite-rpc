use dashmap::DashMap;
use log::{debug, error, info, trace, warn};
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use quinn::Endpoint;
use solana_lite_rpc_core::{
    quic_connection::{PooledConnection, QuicConnectionPool},
    quic_connection_utils::{QuicConnectionParameters, QuicConnectionUtils},
    stores::data_cache::DataCache,
    structures::{
        identity_stakes::IdentityStakesData, rotating_queue::RotatingQueue,
    },
};
use solana_sdk::pubkey::Pubkey;
use solana_streamer::nonblocking::quic::compute_max_allowed_uni_streams;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use std::time::Duration;
use itertools::Itertools;
use spl_memo::id;
use tokio::sync::{broadcast::Receiver, broadcast::Sender};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use solana_lite_rpc_core::atomic_timing::AtomicTiming;

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



// TODO
const MAXIMUM_TRANSACTIONS_IN_QUEUE: usize = 16_384;

// FIXME
const SHUTDOWN_AGENT_THRESHOLD: Duration = Duration::from_millis(2500);



pub type WireTransaction = Vec<u8>;

#[derive(Debug, Clone)]
pub enum BroadcastMessage {
    Transaction(WireTransaction),
    Shutdown(Pubkey),
}

struct ActiveConnection {
    endpoints: RotatingQueue<Endpoint>,
    identity: Pubkey,
    tpu_address: SocketAddr,
    exit_signal: Arc<AtomicBool>,
    data_cache: DataCache,
    connection_parameters: QuicConnectionParameters,
    last_used: Arc<AtomicTiming>,
}

impl ActiveConnection {
    pub fn new(
        endpoints: RotatingQueue<Endpoint>,
        tpu_address: SocketAddr,
        identity: Pubkey,
        data_cache: DataCache,
        connection_parameters: QuicConnectionParameters,
    ) -> Self {
        let now = Instant::now();
        Self {
            endpoints,
            tpu_address,
            identity,
            exit_signal: Arc::new(AtomicBool::new(false)),
            data_cache,
            connection_parameters,
            last_used: Arc::new(AtomicTiming::default()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn listen(
        connection_pool: QuicConnectionPool,
        broadcast_receiver: Receiver<BroadcastMessage>,
        exit_signal: Arc<AtomicBool>,
        identity: Pubkey,
        addr: SocketAddr,
        last_used: Arc<AtomicTiming>,
    ) {
        NB_QUIC_ACTIVE_CONNECTIONS.inc();
        let mut broadcast_receiver = broadcast_receiver;

        loop {

            tokio::select! {
                broadcast_message = broadcast_receiver.recv() => {

                    let tx: Vec<u8> = match broadcast_message {
                        Ok(BroadcastMessage::Transaction(tx_raw)) => {
                            last_used.update();
                            tx_raw
                        },
                        Ok(BroadcastMessage::Shutdown(tpu_identity)) => {
                            if tpu_identity == identity {
                                debug!("Received shutdown signal for active connection to {}", addr);
                                exit_signal.store(true, Ordering::Relaxed);
                                break;
                            }
                            continue;
                        },
                        Err(e) => {
                            error!(
                                "Broadcast channel error on recv for {} error {} - continue",
                                addr, e
                            );
                            continue;
                        }
                    };

                    let PooledConnection {
                        connection,
                        permit
                    } = match connection_pool.get_pooled_connection().await {
                        Ok(connection_pool) => connection_pool,
                        Err(e) => {
                            error!("error getting pooled connection {e:?}");
                            break;
                        },
                    };

                    tokio::spawn(async move {
                        // permit will be used to send all the transaction and then destroyed
                        let _permit = permit;
                        NB_QUIC_TASKS.inc();
                        connection.send_transaction(tx).await;
                        NB_QUIC_TASKS.dec();
                    });
                },
            }
        }
        drop(broadcast_receiver);
        NB_QUIC_CONNECTIONS.dec();
        NB_QUIC_ACTIVE_CONNECTIONS.dec();
    }

    pub fn start_listening(
        &self,
        broadcast_receiver: Receiver<BroadcastMessage>,
        identity_stakes: IdentityStakesData,
    ) {
        let addr = self.tpu_address;

        let max_number_of_connections = self.connection_parameters.max_number_of_connections;

        let max_uni_stream_connections = compute_max_allowed_uni_streams(
            identity_stakes.peer_type,
            identity_stakes.stakes,
            identity_stakes.total_stakes,
        );
        let exit_signal = self.exit_signal.clone();

        let connection_pool = QuicConnectionPool::new(
            self.identity.clone(),
            self.endpoints.clone(),
            addr,
            self.connection_parameters,
            exit_signal.clone(),
            max_number_of_connections,
            max_uni_stream_connections,
        );

        tokio::spawn(
            Self::listen(
                connection_pool.clone(),
                broadcast_receiver,
                exit_signal.clone(),
                self.identity,
                addr.clone(),
                self.last_used.clone(),
            ));

    }
}

pub struct TpuConnectionManager {
    endpoints: RotatingQueue<Endpoint>,
    identity_to_active_connection: Arc<DashMap<Pubkey, Arc<ActiveConnection>>>,
    // channel to communicate to the active connections threads
    broadcast_sender: Arc<Sender<BroadcastMessage>>,
}

impl TpuConnectionManager {
    pub fn send_transaction(&self, transaction: Vec<u8>) {
        self.broadcast_sender.send(BroadcastMessage::Transaction(transaction))
            .expect("failed to send to broadcast");
    }
}

impl TpuConnectionManager {
    pub async fn new(
        certificate: rustls::Certificate,
        key: rustls::PrivateKey,
        fanout: usize,
    ) -> Self {

        let (sender, _) = tokio::sync::broadcast::channel::<BroadcastMessage>(MAXIMUM_TRANSACTIONS_IN_QUEUE);
        let broadcast_sender = Arc::new(sender.clone());
        let broadcast_sender2 = Arc::new(sender.clone()); // TODO do we need this?
        drop(sender);

        let identity_to_active_connection = Arc::new(DashMap::new());

        tokio::spawn(
            Self::cleanup_unused_connections(
            identity_to_active_connection.clone(),
            broadcast_sender2,
            ));

        let number_of_clients = fanout * 2;
        Self {
            endpoints: RotatingQueue::new(number_of_clients, || {
                QuicConnectionUtils::create_endpoint(certificate.clone(), key.clone())
            }),
            identity_to_active_connection,
            broadcast_sender,
        }
    }

    #[tracing::instrument(skip_all, level = "warn")]
    pub async fn update_connections(
        &self,
        connections_to_keep: &HashMap<Pubkey, SocketAddr>,
        identity_stakes: IdentityStakesData,
        data_cache: DataCache,
        connection_parameters: QuicConnectionParameters,
    ) {
        NB_CONNECTIONS_TO_KEEP.set(connections_to_keep.len() as i64);
        for (tpu_identity, tpu_addr) in connections_to_keep {
            if self.identity_to_active_connection.get(tpu_identity).is_some() {
                continue;
            }
            debug!("add active connection for {}, {}", tpu_identity, tpu_addr);
            let active_connection = ActiveConnection::new(
                self.endpoints.clone(),
                *tpu_addr,
                *tpu_identity,
                data_cache.clone(),
                connection_parameters,
            );

            let broadcast_receiver = self.broadcast_sender.subscribe();

            active_connection.start_listening(broadcast_receiver, identity_stakes);
            self.identity_to_active_connection.insert(
                *tpu_identity,
                Arc::new(active_connection),
            );
        }

    }

    async fn cleanup_unused_connections(
        identity_to_active_connection: Arc<DashMap<Pubkey, Arc<ActiveConnection>>>,
        broadcast_sender: Arc<Sender<BroadcastMessage>>,
    ) {

        let mut period = tokio::time::interval(std::time::Duration::from_millis(500));

        loop {
            period.tick().await;

            let connections_to_shutdown =
                identity_to_active_connection.iter()
                    .filter(|x| {
                        let elapsed = x.last_used.elapsed();
                        elapsed > SHUTDOWN_AGENT_THRESHOLD
                    }).map(|x| *x.key())
                    .collect_vec();

            for identity in connections_to_shutdown {
                // entry must exist because nobody else can remove it
                broadcast_sender.send(BroadcastMessage::Shutdown(identity)).expect("failed to send to broadcast");
                identity_to_active_connection.remove(&identity);
                debug!("send shutdown message for active connection to tpu {}", identity);
            }

        }
    }

}

