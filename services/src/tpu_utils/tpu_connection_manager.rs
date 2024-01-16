use dashmap::DashMap;
use log::{error, trace};
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use quinn::Endpoint;
use solana_lite_rpc_core::{
    stores::data_cache::DataCache,
    structures::{
        identity_stakes::IdentityStakesData, rotating_queue::RotatingQueue,
        transaction_sent_info::SentTransactionInfo,
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
use tokio::sync::{broadcast::Receiver, broadcast::Sender};

use crate::{
    quic_connection::{PooledConnection, QuicConnectionPool},
    quic_connection_utils::{QuicConnectionParameters, QuicConnectionUtils},
};

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

#[derive(Clone)]
struct ActiveConnection {
    endpoints: RotatingQueue<Endpoint>,
    identity: Pubkey,
    tpu_address: SocketAddr,
    exit_signal: Arc<AtomicBool>,
    data_cache: DataCache,
    connection_parameters: QuicConnectionParameters,
}

impl ActiveConnection {
    pub fn new(
        endpoints: RotatingQueue<Endpoint>,
        tpu_address: SocketAddr,
        identity: Pubkey,
        data_cache: DataCache,
        connection_parameters: QuicConnectionParameters,
    ) -> Self {
        Self {
            endpoints,
            tpu_address,
            identity,
            exit_signal: Arc::new(AtomicBool::new(false)),
            data_cache,
            connection_parameters,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn listen(
        &self,
        transaction_reciever: Receiver<SentTransactionInfo>,
        exit_oneshot_channel: tokio::sync::mpsc::Receiver<()>,
        addr: SocketAddr,
        identity_stakes: IdentityStakesData,
    ) {
        NB_QUIC_ACTIVE_CONNECTIONS.inc();
        let mut transaction_reciever = transaction_reciever;
        let mut exit_oneshot_channel = exit_oneshot_channel;
        let identity = self.identity;

        let max_number_of_connections = self.connection_parameters.max_number_of_connections;

        let max_uni_stream_connections = compute_max_allowed_uni_streams(
            identity_stakes.peer_type,
            identity_stakes.stakes,
            identity_stakes.total_stakes,
        );
        let exit_signal = self.exit_signal.clone();
        let connection_pool = QuicConnectionPool::new(
            identity,
            self.endpoints.clone(),
            addr,
            self.connection_parameters,
            exit_signal.clone(),
            max_number_of_connections,
            max_uni_stream_connections,
        );

        loop {
            // exit signal set
            if exit_signal.load(Ordering::Relaxed) {
                break;
            }

            tokio::select! {
                tx = transaction_reciever.recv() => {
                    // exit signal set
                    if exit_signal.load(Ordering::Relaxed) {
                        break;
                    }

                    let tx: Vec<u8> = match tx {
                        Ok(transaction_sent_info) => {
                            if self.data_cache.txs.is_transaction_confirmed(&transaction_sent_info.signature) {
                                // transaction is already confirmed/ no need to send
                                continue;
                            }
                            transaction_sent_info.transaction
                        },
                        Err(e) => {
                            error!(
                                "Broadcast channel error on recv for {} error {} - continue",
                                identity, e
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
                _ = exit_oneshot_channel.recv() => {
                    break;
                }
            }
        }
        drop(transaction_reciever);
        NB_QUIC_CONNECTIONS.dec();
        NB_QUIC_ACTIVE_CONNECTIONS.dec();
    }

    pub fn start_listening(
        &self,
        transaction_reciever: Receiver<SentTransactionInfo>,
        exit_oneshot_channel: tokio::sync::mpsc::Receiver<()>,
        identity_stakes: IdentityStakesData,
    ) {
        let addr = self.tpu_address;
        let this = self.clone();
        tokio::spawn(async move {
            this.listen(
                transaction_reciever,
                exit_oneshot_channel,
                addr,
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
    pub async fn new(
        certificate: rustls::Certificate,
        key: rustls::PrivateKey,
        fanout: usize,
    ) -> Self {
        let number_of_clients = fanout * 4;
        Self {
            endpoints: RotatingQueue::new(number_of_clients, || {
                QuicConnectionUtils::create_endpoint(certificate.clone(), key.clone())
            }),
            identity_to_active_connection: Arc::new(DashMap::new()),
        }
    }

    pub async fn update_connections(
        &self,
        broadcast_sender: Arc<Sender<SentTransactionInfo>>,
        connections_to_keep: HashMap<Pubkey, SocketAddr>,
        identity_stakes: IdentityStakesData,
        data_cache: DataCache,
        connection_parameters: QuicConnectionParameters,
    ) {
        NB_CONNECTIONS_TO_KEEP.set(connections_to_keep.len() as i64);
        for (identity, socket_addr) in &connections_to_keep {
            if self.identity_to_active_connection.get(identity).is_none() {
                trace!("added a connection for {}, {}", identity, socket_addr);
                let active_connection = ActiveConnection::new(
                    self.endpoints.clone(),
                    *socket_addr,
                    *identity,
                    data_cache.clone(),
                    connection_parameters,
                );
                // using mpsc as a oneshot channel/ because with one shot channel we cannot reuse the reciever
                let (sx, rx) = tokio::sync::mpsc::channel(1);

                let broadcast_receiver = broadcast_sender.subscribe();
                active_connection.start_listening(broadcast_receiver, rx, identity_stakes);
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
