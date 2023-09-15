use dashmap::DashMap;
use log::{error, trace};
use quinn::Endpoint;
use solana_lite_rpc_core::{
    quic_connection::QuicConnectionPool,
    quic_connection_utils::{QuicConnectionParameters, QuicConnectionUtils},
    rotating_queue::RotatingQueue,
    structures::identity_stakes::IdentityStakesData,
    tx_store::TxStore,
};
use solana_sdk::pubkey::Pubkey;
use solana_streamer::nonblocking::quic::compute_max_allowed_uni_streams;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::{broadcast::Receiver, broadcast::Sender};

#[derive(Clone)]
struct ActiveConnection {
    endpoints: RotatingQueue<Endpoint>,
    identity: Pubkey,
    tpu_address: SocketAddr,
    exit_signal: Arc<AtomicBool>,
    txs_sent_store: TxStore,
    connection_parameters: QuicConnectionParameters,
}

impl ActiveConnection {
    pub fn new(
        endpoints: RotatingQueue<Endpoint>,
        tpu_address: SocketAddr,
        identity: Pubkey,
        txs_sent_store: TxStore,
        connection_parameters: QuicConnectionParameters,
    ) -> Self {
        Self {
            endpoints,
            tpu_address,
            identity,
            exit_signal: Arc::new(AtomicBool::new(false)),
            txs_sent_store,
            connection_parameters,
        }
    }

    fn check_for_confirmation(txs_sent_store: &TxStore, signature: String) -> bool {
        match txs_sent_store.get(&signature) {
            Some(props) => props.status.is_some(),
            None => false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn listen(
        &self,
        transaction_reciever: Receiver<(String, Vec<u8>)>,
        exit_oneshot_channel: tokio::sync::mpsc::Receiver<()>,
        addr: SocketAddr,
        identity_stakes: IdentityStakesData,
        txs_sent_store: TxStore,
    ) {
        let mut transaction_reciever = transaction_reciever;
        let mut exit_oneshot_channel = exit_oneshot_channel;
        let identity = self.identity;

        let max_uni_stream_connections: u64 = compute_max_allowed_uni_streams(
            identity_stakes.peer_type,
            identity_stakes.stakes,
            identity_stakes.total_stakes,
        ) as u64;
        let number_of_transactions_per_unistream = self
            .connection_parameters
            .number_of_transactions_per_unistream;
        let max_number_of_connections = self.connection_parameters.max_number_of_connections;

        let task_counter: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
        let exit_signal = self.exit_signal.clone();
        let connection_pool = QuicConnectionPool::new(
            identity,
            self.endpoints.clone(),
            addr,
            self.connection_parameters,
            exit_signal.clone(),
        );

        loop {
            // exit signal set
            if exit_signal.load(Ordering::Relaxed) {
                break;
            }

            if task_counter.load(Ordering::Relaxed) >= max_uni_stream_connections {
                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                continue;
            }

            tokio::select! {
                tx = transaction_reciever.recv() => {
                    // exit signal set
                    if exit_signal.load(Ordering::Relaxed) {
                        break;
                    }

                    let first_tx: Vec<u8> = match tx {
                        Ok((sig, tx)) => {
                            if Self::check_for_confirmation(&txs_sent_store, sig) {
                                // transaction is already confirmed/ no need to send
                                continue;
                            }
                            tx
                        },
                        Err(e) => {
                            error!(
                                "Broadcast channel error on recv for {} error {} - continue",
                                identity, e
                            );
                            break;
                        }
                    };

                    let mut txs = vec![first_tx];
                    for _ in 1..number_of_transactions_per_unistream {
                        if let Ok((sig, tx)) = transaction_reciever.try_recv() {
                            if Self::check_for_confirmation(&txs_sent_store, sig) {
                                continue;
                            }
                            txs.push(tx);
                        }
                    }

                    // queue getting full and a connection poll is getting slower
                    // add more connections to the pool
                    if connection_pool.len() < max_number_of_connections {
                        connection_pool.add_connection().await;
                    }

                    let task_counter = task_counter.clone();
                    let connection_pool = connection_pool.clone();

                    tokio::spawn(async move {
                        task_counter.fetch_add(1, Ordering::Relaxed);
                        connection_pool.send_transaction_batch(txs).await;
                        task_counter.fetch_sub(1, Ordering::Relaxed);
                    });
                },
                _ = exit_oneshot_channel.recv() => {
                    break;
                }
            }
        }
        drop(transaction_reciever);
    }

    pub fn start_listening(
        &self,
        transaction_reciever: Receiver<(String, Vec<u8>)>,
        exit_oneshot_channel: tokio::sync::mpsc::Receiver<()>,
        identity_stakes: IdentityStakesData,
    ) {
        let addr = self.tpu_address;
        let txs_sent_store = self.txs_sent_store.clone();
        let this = self.clone();
        tokio::spawn(async move {
            this.listen(
                transaction_reciever,
                exit_oneshot_channel,
                addr,
                identity_stakes,
                txs_sent_store,
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
        let number_of_clients = fanout * 2;
        Self {
            endpoints: RotatingQueue::new(number_of_clients, || {
                QuicConnectionUtils::create_endpoint(certificate.clone(), key.clone())
            })
            .await,
            identity_to_active_connection: Arc::new(DashMap::new()),
        }
    }

    pub async fn update_connections(
        &self,
        broadcast_sender: Arc<Sender<(String, Vec<u8>)>>,
        connections_to_keep: HashMap<Pubkey, SocketAddr>,
        identity_stakes: IdentityStakesData,
        txs_sent_store: TxStore,
        connection_parameters: QuicConnectionParameters,
    ) {
        for (identity, socket_addr) in &connections_to_keep {
            if self.identity_to_active_connection.get(identity).is_none() {
                trace!("added a connection for {}, {}", identity, socket_addr);
                let active_connection = ActiveConnection::new(
                    self.endpoints.clone(),
                    *socket_addr,
                    *identity,
                    txs_sent_store.clone(),
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
