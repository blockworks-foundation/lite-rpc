use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use anyhow::bail;
use log::{error, info, warn};
use prometheus::{opts, register_int_gauge};
use prometheus::core::GenericGauge;
use quinn::{Connection, Endpoint};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;
use solana_streamer::nonblocking::quic::compute_max_allowed_uni_streams;
use tokio::sync::broadcast::Receiver;
use tokio::sync::RwLock;
use tokio::time::timeout;
use crate::identity_stakes::IdentityStakes;
use crate::proxy_request_format::TpuForwardingRequest;
use crate::quic_connection_utils::QuicConnectionUtils;
use crate::tpu_quic_connection::{CONNECTION_RETRY_COUNT, QUIC_CONNECTION_TIMEOUT};
use crate::tx_store::TxStore;
use itertools::Itertools;

lazy_static::lazy_static! {
    // TODO rename / cleanup
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
        // TODO build a smarter duplication check
        false
        // match txs_sent_store.get(&signature) {
        //     Some(props) => props.status.is_some(),
        //     None => false,
        // }
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

                        NB_QUIC_TASKS.dec();
                        task_counter.fetch_sub(1, Ordering::Relaxed);
                    });
                },
                _ = exit_oneshot_channel.recv() => {
                    break;
                }
            };
        }
        drop(transaction_reciever);
        NB_QUIC_CONNECTIONS.dec();
        NB_QUIC_ACTIVE_CONNECTIONS.dec();
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
