use dashmap::DashMap;
use log::{debug, error, info, trace, warn};
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use quinn::{Connection, Endpoint};
use solana_lite_rpc_core::{
    quic_connection_utils::QuicConnectionUtils, rotating_queue::RotatingQueue,
    structures::identity_stakes::IdentityStakes, tx_store::TxStore,
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
    time::Duration,
};
use anyhow::bail;
use itertools::Itertools;
use solana_client::client_error::reqwest::header::WARNING;
use solana_sdk::transaction::VersionedTransaction;
use tokio::sync::{broadcast::Receiver, broadcast::Sender, RwLock};
use tokio::time::timeout;
use solana_lite_rpc_core::proxy_request_format::TpuForwardingRequest;

pub const QUIC_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
pub const CONNECTION_RETRY_COUNT: usize = 10;

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

pub struct ActiveConnection {
    endpoint: Endpoint,
    identity: Pubkey,
    tpu_address: SocketAddr,
    exit_signal: Arc<AtomicBool>,
    txs_sent_store: TxStore,
}

impl ActiveConnection {
    pub fn new(
        endpoint: Endpoint,
        tpu_address: SocketAddr,
        identity: Pubkey,
        txs_sent_store: TxStore,
    ) -> Self {
        Self {
            endpoint,
            tpu_address,
            identity,
            exit_signal: Arc::new(AtomicBool::new(false)),
            txs_sent_store,
        }
    }

    fn on_connect() {
        NB_QUIC_CONNECTIONS.inc();
    }

    fn check_for_confirmation(txs_sent_store: &TxStore, signature: String) -> bool {
        match txs_sent_store.get(&signature) {
            Some(props) => props.status.is_some(),
            None => false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn listen(
        transaction_reciever: Receiver<(String, Vec<u8>)>,
        exit_oneshot_channel: tokio::sync::mpsc::Receiver<()>,
        endpoint: Endpoint,
        tpu_address: SocketAddr,
        exit_signal: Arc<AtomicBool>,
        identity: Pubkey,
        identity_stakes: IdentityStakes,
        txs_sent_store: TxStore,
    ) {
        debug!("listen with active connection for identity {} to tpu address {}", identity, tpu_address);
        NB_QUIC_ACTIVE_CONNECTIONS.inc();
        let mut transaction_reciever = transaction_reciever;
        let mut exit_oneshot_channel = exit_oneshot_channel;

        let max_uni_stream_connections: u64 = compute_max_allowed_uni_streams(
            identity_stakes.peer_type,
            identity_stakes.stakes,
            identity_stakes.total_stakes,
        ) as u64;
        let number_of_transactions_per_unistream = 5;

        let task_counter: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
        let mut connection: Option<Arc<RwLock<Connection>>> = None;
        let last_stable_id: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

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
                                "Broadcast channel error on recv for {} error {}",
                                identity, e
                            );
                            continue;
                        }
                    };

                    let mut txs = vec![first_tx];
                    for _ in 1..number_of_transactions_per_unistream {
                        if let Ok((signature, tx)) = transaction_reciever.try_recv() {
                            if Self::check_for_confirmation(&txs_sent_store, signature) {
                                continue;
                            }
                            txs.push(tx);
                        }
                    }

                    if connection.is_none() {
                        // initial connection
                        let conn = QuicConnectionUtils::connect(
                            identity,
                            false,
                            endpoint.clone(),
                            tpu_address,
                            QUIC_CONNECTION_TIMEOUT,
                            CONNECTION_RETRY_COUNT,
                            exit_signal.clone(),
                            Self::on_connect).await;

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
                    let connection = connection.clone();
                    let last_stable_id = last_stable_id.clone();

                    tokio::spawn(async move {
                        task_counter.fetch_add(1, Ordering::Relaxed);
                        NB_QUIC_TASKS.inc();
                        let connection = connection.unwrap();

                        if false {
                            // TODO split to new service
                            // SOS
                            info!("Sending copy of transaction batch of {} to tpu with identity {} to quic proxy",
                                txs.len(), identity);
                            Self::send_copy_of_txs_to_quicproxy(
                                &txs, endpoint.clone(),
                                // proxy address
                                "127.0.0.1:11111".parse().unwrap(),
                                tpu_address,
                                identity.clone()).await.unwrap();
                        }


                        if true {
                            QuicConnectionUtils::send_transaction_batch(
                                connection,
                                txs,
                                identity,
                                endpoint,
                                tpu_address,
                                exit_signal,
                                last_stable_id,
                                QUIC_CONNECTION_TIMEOUT,
                                CONNECTION_RETRY_COUNT,
                                || {
                                    // do nothing as we are using the same connection
                                }
                            ).await;
                        }

                        NB_QUIC_TASKS.dec();
                        task_counter.fetch_sub(1, Ordering::Relaxed);
                    });
                },
                _ = exit_oneshot_channel.recv() => {
                    break;
                }
            }
            ;
        }
        drop(transaction_reciever);
        NB_QUIC_CONNECTIONS.dec();
        NB_QUIC_ACTIVE_CONNECTIONS.dec();
    }

    async fn send_copy_of_txs_to_quicproxy(raw_tx_batch: &Vec<Vec<u8>>, endpoint: Endpoint,
                                           proxy_address: SocketAddr, tpu_target_address: SocketAddr,
                                           identity: Pubkey) -> anyhow::Result<()> {
        info!("sending vecvec: {}", raw_tx_batch.iter().map(|tx| tx.len()).into_iter().join(","));

        let raw_tx_batch_copy = raw_tx_batch.clone();

        let mut txs = vec![];

        for raw_tx in raw_tx_batch_copy {
            let tx = match bincode::deserialize::<VersionedTransaction>(&raw_tx) {
                Ok(tx) => tx,
                Err(err) => {
                    bail!(err.to_string());
                }
            };
            txs.push(tx);
        }

        let forwarding_request = TpuForwardingRequest::new(tpu_target_address, identity, txs);

        let proxy_request_raw = bincode::serialize(&forwarding_request).expect("Expect to serialize transactions");

        let send_result = timeout(Duration::from_millis(3500), Self::send_proxy_request(endpoint, proxy_address, &proxy_request_raw));

        match send_result.await {
            Ok(..) => {
                info!("Successfully sent data to quic proxy");
            }
            Err(e) => {
                warn!("Failed to send data to quic proxy: {:?}", e);
            }
        }
        Ok(())
    }

    async fn send_proxy_request(endpoint: Endpoint, proxy_address: SocketAddr, proxy_request_raw: &Vec<u8>) -> anyhow::Result<()> {
        info!("sending {} bytes to proxy", proxy_request_raw.len());

        let mut connecting = endpoint.connect(proxy_address, "localhost")?;
        let connection = timeout(Duration::from_millis(500), connecting).await??;
        let mut send = connection.open_uni().await?;

        send.write_all(proxy_request_raw).await?;

        send.finish().await?;

        Ok(())
    }

    pub fn start_listening(
        &self,
        transaction_reciever: Receiver<(String, Vec<u8>)>,
        exit_oneshot_channel: tokio::sync::mpsc::Receiver<()>,
        identity_stakes: IdentityStakes,
    ) {
        let endpoint = self.endpoint.clone();
        let tpu_address = self.tpu_address;
        let exit_signal = self.exit_signal.clone();
        let identity = self.identity;
        let txs_sent_store = self.txs_sent_store.clone();
        tokio::spawn(async move {
            Self::listen(
                transaction_reciever,
                exit_oneshot_channel,
                endpoint,
                tpu_address,
                exit_signal,
                identity,
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
    pub fn new(certificate: rustls::Certificate, key: rustls::PrivateKey, fanout: usize) -> Self {
        let number_of_clients = if fanout > 5 { fanout / 4 } else { 1 };
        Self {
            endpoints: RotatingQueue::new(number_of_clients, || {
                QuicConnectionUtils::create_endpoint(certificate.clone(), key.clone())
            }),
            identity_to_active_connection: Arc::new(DashMap::new()),
        }
    }

    pub async fn update_connections(
        &self,
        transaction_sender: Arc<Sender<(String, Vec<u8>)>>,
        connections_to_keep: HashMap<Pubkey, SocketAddr>,
        identity_stakes: IdentityStakes,
        txs_sent_store: TxStore,
    ) {
        NB_CONNECTIONS_TO_KEEP.set(connections_to_keep.len() as i64);
        for (identity, socket_addr) in &connections_to_keep {
            if self.identity_to_active_connection.get(identity).is_none() {
                trace!("added a connection for {}, {}", identity, socket_addr);
                let endpoint = self.endpoints.get();
                let active_connection = ActiveConnection::new(
                    endpoint,
                    *socket_addr,
                    *identity,
                    txs_sent_store.clone(),
                );
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
