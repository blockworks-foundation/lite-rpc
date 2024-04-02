use dashmap::DashMap;
use log::{error, trace};
use prometheus::{
    core::GenericGauge, histogram_opts, opts, register_histogram, register_int_gauge, Histogram,
};
use quinn::Endpoint;
use solana_lite_rpc_core::{
    stores::data_cache::DataCache,
    structures::{
        identity_stakes::IdentityStakesData, prioritization_fee_heap::PrioritizationFeesHeap,
        rotating_queue::RotatingQueue, transaction_sent_info::SentTransactionInfo,
    },
};
use solana_sdk::pubkey::Pubkey;
use solana_streamer::nonblocking::quic::compute_max_allowed_uni_streams;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::{
    broadcast::{self, Receiver, Sender},
    Notify,
};

use crate::{
    quic_connection::{PooledConnection, QuicConnectionPool},
    quic_connection_utils::{QuicConnectionParameters, QuicConnectionUtils},
};

lazy_static::lazy_static! {
    static ref NB_QUIC_ACTIVE_CONNECTIONS: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_nb_active_connections", "Number quic tasks that are running")).unwrap();
    static ref NB_CONNECTIONS_TO_KEEP: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_connections_to_keep", "Number of connections to keep asked by tpu service")).unwrap();
    static ref NB_QUIC_TASKS: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_quic_tasks", "Number of connections to keep asked by tpu service")).unwrap();
    static ref TT_SENT_TIMER: Histogram = register_histogram!(histogram_opts!(
            "literpc_txs_send_timer",
            "Time to send transaction batch",
        ))
        .unwrap();

    static ref TRANSACTIONS_IN_HEAP: GenericGauge<prometheus::core::AtomicI64> =
        register_int_gauge!(opts!("literpc_transactions_in_priority_heap", "Number of transactions in priority heap")).unwrap();
}

#[derive(Clone)]
struct ActiveConnection {
    endpoints: RotatingQueue<Endpoint>,
    identity: Pubkey,
    tpu_address: SocketAddr,
    data_cache: DataCache,
    connection_parameters: QuicConnectionParameters,
    exit_notifier: broadcast::Sender<()>,
}

impl ActiveConnection {
    pub fn new(
        endpoints: RotatingQueue<Endpoint>,
        tpu_address: SocketAddr,
        identity: Pubkey,
        data_cache: DataCache,
        connection_parameters: QuicConnectionParameters,
    ) -> Self {
        let (exit_notifier, _) = broadcast::channel(1);
        Self {
            endpoints,
            tpu_address,
            identity,
            data_cache,
            connection_parameters,
            exit_notifier,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn listen(
        &self,
        mut transaction_reciever: Receiver<SentTransactionInfo>,
        addr: SocketAddr,
        identity_stakes: IdentityStakesData,
    ) {
        let fill_notify = Arc::new(Notify::new());

        let identity = self.identity;

        NB_QUIC_ACTIVE_CONNECTIONS.inc();

        let max_number_of_connections = self.connection_parameters.max_number_of_connections;

        let max_uni_stream_connections = compute_max_allowed_uni_streams(
            identity_stakes.peer_type,
            identity_stakes.stakes,
            identity_stakes.total_stakes,
        );
        let connection_pool = QuicConnectionPool::new(
            identity,
            self.endpoints.clone(),
            addr,
            self.connection_parameters,
            max_number_of_connections,
            max_uni_stream_connections,
        );
        let prioritization_heap_size = self
            .connection_parameters
            .prioritization_heap_size
            .unwrap_or(2 * max_uni_stream_connections);
        let priorization_heap = PrioritizationFeesHeap::new(prioritization_heap_size);

        let heap_filler_task = {
            let priorization_heap = priorization_heap.clone();
            let data_cache = self.data_cache.clone();
            let fill_notify = fill_notify.clone();
            let mut exit_notifier = self.exit_notifier.subscribe();
            tokio::spawn(async move {
                let mut current_blockheight =
                    data_cache.block_information_store.get_last_blockheight();
                loop {
                    let tx = tokio::select! {
                        tx = transaction_reciever.recv() => {
                            tx
                        },
                        _ = exit_notifier.recv() => {
                            break;
                        }
                    };
                    match tx {
                        Ok(transaction_sent_info) => {
                            if data_cache
                                .check_if_confirmed_or_expired_blockheight(&transaction_sent_info)
                            {
                                // transactions is confirmed or expired
                                continue;
                            }

                            priorization_heap.insert(transaction_sent_info).await;
                            TRANSACTIONS_IN_HEAP.inc();

                            fill_notify.notify_one();
                            // give little more priority to read the transaction sender with this wait
                            let last_blockheight =
                                data_cache.block_information_store.get_last_blockheight();
                            if last_blockheight != current_blockheight {
                                current_blockheight = last_blockheight;
                                // give more priority to transaction sender
                                tokio::time::sleep(Duration::from_micros(50)).await;
                                // remove all expired transactions from the queue
                                let elements_removed = priorization_heap
                                    .remove_expired_transactions(current_blockheight)
                                    .await;
                                TRANSACTIONS_IN_HEAP.sub(elements_removed as i64);
                            }
                        }
                        Err(e) => {
                            error!(
                                "Broadcast channel error on recv for {} error {} - continue",
                                identity, e
                            );
                            continue;
                        }
                    };
                }
            })
        };

        // create atleast one connection before waiting from transactions
        if let Ok(PooledConnection { connection, permit }) =
            connection_pool.get_pooled_connection().await
        {
            let exit_notifier = self.exit_notifier.subscribe();
            tokio::task::spawn(async move {
                let _permit = permit;
                connection.get_connection(exit_notifier).await;
            });
        };

        let mut exit_notifier = self.exit_notifier.subscribe();
        'main_loop: loop {
            tokio::select! {
                _ = fill_notify.notified() => {

                    'process_heap: loop {
                        let Some(tx) = priorization_heap.pop().await else {
                            // wait to get notification from fill event
                            break 'process_heap;
                        };
                        TRANSACTIONS_IN_HEAP.dec();

                        // check if transaction is already confirmed
                        if self.data_cache.txs.is_transaction_confirmed(&tx.signature) {
                            continue;
                        }

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
                        let exit_notifier = self.exit_notifier.subscribe();

                        tokio::spawn(async move {
                            // permit will be used to send all the transaction and then destroyed
                            let _permit = permit;
                            let timer = TT_SENT_TIMER.start_timer();

                            NB_QUIC_TASKS.inc();

                            connection.send_transaction(tx.transaction.as_ref(), exit_notifier).await;
                            timer.observe_duration();
                            NB_QUIC_TASKS.dec();
                        });
                    }
                },
                _ = exit_notifier.recv() => {
                    break 'main_loop;
                }
            }
        }

        let _ = heap_filler_task.await;
        let elements_removed = priorization_heap.clear().await;
        TRANSACTIONS_IN_HEAP.sub(elements_removed as i64);
        NB_QUIC_ACTIVE_CONNECTIONS.dec();
    }

    pub fn start_listening(
        &self,
        transaction_reciever: Receiver<SentTransactionInfo>,
        identity_stakes: IdentityStakesData,
    ) {
        let addr = self.tpu_address;
        let this = self.clone();
        tokio::spawn(async move {
            this.listen(transaction_reciever, addr, identity_stakes)
                .await;
        });
    }
}

pub struct TpuConnectionManager {
    endpoints: RotatingQueue<Endpoint>,
    identity_to_active_connection: Arc<DashMap<Pubkey, ActiveConnection>>,
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
                let broadcast_receiver = broadcast_sender.subscribe();
                active_connection.start_listening(broadcast_receiver, identity_stakes);
                self.identity_to_active_connection
                    .insert(*identity, active_connection);
            }
        }

        // remove connections which are no longer needed
        self.identity_to_active_connection.retain(|key, value| {
            if !connections_to_keep.contains_key(key) {
                trace!("removing a connection for {}", key.to_string());
                // ignore error for exit channel
                let _ = value.exit_notifier.send(());
                false
            } else {
                true
            }
        });
    }
}
