use core::panic;
use prometheus::{
    core::GenericGauge, histogram_opts, opts, register_histogram, register_int_gauge, Histogram,
};
use solana_lite_rpc_core::structures::transaction_sent_info::SentTransactionInfo;
use std::{net::SocketAddr, thread::JoinHandle};

use super::quiche::{quiche_transaction_sending_loop, QuicheMessage};

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

pub fn start_quiche_transaction_sending_thread(
    number_of_unistream: u64,
    message_channel: mio_channel::Receiver<QuicheMessage>,
    certificate: rustls::Certificate,
    key: rustls::PrivateKey,
    prioritization_heap_size: Option<usize>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        if let Err(e) = quiche_transaction_sending_loop(
            number_of_unistream,
            message_channel,
            certificate,
            key,
            prioritization_heap_size,
        ) {
            log::error!("Error in quiche_transaction_sending_loop: {:?}", e);
            panic!("Error in quiche_transaction_sending_loop: {:?}", e);
        }
    })
}

pub struct TpuConnectionManager {
    _quiche_thread: JoinHandle<()>,
    _transaction_convertor_thread: JoinHandle<()>,
    message_channel: mio_channel::Sender<QuicheMessage>,
}

impl TpuConnectionManager {
    pub async fn new(
        certificate: rustls::Certificate,
        key: rustls::PrivateKey,
        max_unistreams: u64,
        transaction_messages: std::sync::mpsc::Receiver<SentTransactionInfo>,
    ) -> Self {
        let (message_sender, message_channel) = mio_channel::channel();

        let transaction_convertor_thread = {
            let message_sender = message_sender.clone();
            std::thread::spawn(move || {
                while let Ok(transaction) = transaction_messages.recv() {
                    // convert transaction to quiche message
                    let message = QuicheMessage::SendTransaction(transaction);
                    message_sender.send(message).unwrap();
                }
            })
        };
        let quiche_thread = start_quiche_transaction_sending_thread(
            max_unistreams,
            message_channel,
            certificate,
            key,
            None,
        );
        Self {
            _quiche_thread: quiche_thread,
            _transaction_convertor_thread: transaction_convertor_thread,
            message_channel: message_sender,
        }
    }

    pub async fn update_connections(&self, connections_to_keep: Vec<SocketAddr>) {
        NB_CONNECTIONS_TO_KEEP.set(connections_to_keep.len() as i64);
        let message = QuicheMessage::ResetConnections(connections_to_keep);
        self.message_channel.send(message).unwrap();
    }
}
