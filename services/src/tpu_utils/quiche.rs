use std::{
    collections::{BTreeMap, BTreeSet},
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use anyhow::bail;
use boring::ssl::SslMethod;
use log::{debug, error, trace};
use quiche::Config;
use ring::rand::{SecureRandom, SystemRandom};
use solana_lite_rpc_core::structures::{
    prioritization_fee_heap::PrioritizationFeesHeap, transaction_sent_info::SentTransactionInfo,
};
use solana_streamer::nonblocking::quic::ALPN_TPU_PROTOCOL_ID;

use crate::tpu_utils::quiche_utils::{
    detect_gso, get_next_unidi, send_with_pacing, set_txtime_sockopt,
};

const MAX_DATAGRAM_SIZE: usize = 1280;
const DATAGRAM_RECEIVE_BUFFER_SIZE: u64 = 1024 * 1024;

pub fn configure_client(
    certificate: rustls::Certificate,
    key: rustls::PrivateKey,
    number_of_unistreams: u64,
) -> anyhow::Result<quiche::Config> {
    let mut boring_ssl_context = boring::ssl::SslContextBuilder::new(SslMethod::tls())?;
    let x509 = boring::x509::X509::from_der(&certificate.0)?;
    let pkey = boring::pkey::PKey::private_key_from_der(&key.0)?;
    boring_ssl_context.set_certificate(&x509)?;
    boring_ssl_context.set_private_key(&pkey)?;
    let mut config =
        quiche::Config::with_boring_ssl_ctx_builder(quiche::PROTOCOL_VERSION, boring_ssl_context)
            .unwrap();
    config
        .set_application_protos(&[ALPN_TPU_PROTOCOL_ID])
        .unwrap();

    config.set_max_idle_timeout(10000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(DATAGRAM_RECEIVE_BUFFER_SIZE);
    config.set_initial_max_stream_data_bidi_local(0);
    config.set_initial_max_stream_data_bidi_remote(0);
    config.set_initial_max_stream_data_uni(DATAGRAM_RECEIVE_BUFFER_SIZE);
    config.set_initial_max_streams_bidi(0);
    config.set_initial_max_streams_uni(number_of_unistreams);

    config.set_disable_active_migration(true);
    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);
    config.set_max_connection_window(1024 * 1024);
    config.set_max_stream_window(1024 * 1024);

    config.enable_early_data();
    config.grease(true);
    config.enable_hystart(true);
    config.discover_pmtu(true);

    config.set_active_connection_id_limit(1024);
    config.set_initial_congestion_window_packets(1024);

    config.enable_pacing(true);
    Ok(config)
}

pub enum QuicheMessage {
    ResetConnections(Vec<SocketAddr>),
    SendTransaction(SentTransactionInfo),
    UpdateCurrentBlockheight(u64),
}

struct StreamData {
    pub data: Arc<Vec<u8>>,
    pub written: usize,
}

struct QuicheClient {
    pub conn: quiche::Connection,
    pub server_address: SocketAddr,
    pub stream_sender_map: BTreeMap<u64, StreamData>,
    pub prioritization_fee_heap: PrioritizationFeesHeap,
    pub loss_rate: f64,
    pub has_connected: bool,
    pub max_send_burst: usize,
    pub streams_open: usize,
    pub last_stream_id: u64,
}

fn new_quiche_connection(
    socket: &mio::net::UdpSocket,
    server_address: SocketAddr,
    local_address: SocketAddr,
    config: &mut Config,
    prioritization_heap_size: Option<usize>,
    system_rng: &SystemRandom,
) -> anyhow::Result<QuicheClient> {
    let mut scid = [0; quiche::MAX_CONN_ID_LEN];
    if system_rng.fill(&mut scid[..]).is_err() {
        bail!("Error filling scid");
    }
    log::info!("connecing client with quiche");

    let scid = quiche::ConnectionId::from_ref(&scid);

    let mut conn = quiche::connect(
        Some("transaction processing unit"),
        &scid,
        local_address,
        server_address,
        config,
    )?;

    // sending initial connection request
    {
        let mut out = [0; MAX_DATAGRAM_SIZE];
        let (write, send_info) = conn.send(&mut out)?;

        if let Err(e) = socket.send_to(&out[..write], send_info.to) {
            bail!("send() failed: {:?}", e);
        }
    }

    Ok(QuicheClient {
        conn,
        server_address,
        stream_sender_map: BTreeMap::new(),
        prioritization_fee_heap: PrioritizationFeesHeap::new(
            prioritization_heap_size.unwrap_or(1024),
        ),
        loss_rate: 0.0,
        max_send_burst: MAX_DATAGRAM_SIZE * 10,
        has_connected: false,
        streams_open: 0,
        last_stream_id: 0,
    })
}

fn quiche_send_socket(
    buf: &mut [u8],
    socket: &mio::net::UdpSocket,
    quiche_client: &mut QuicheClient,
    enable_pacing: bool,
    enable_gso: bool,
    continue_write: &mut bool,
) -> std::io::Result<usize> {
    // Reduce max_send_burst by 25% if loss is increasing more than 0.1%.
    let calculated_loss_rate =
        quiche_client.conn.stats().lost as f64 / quiche_client.conn.stats().sent as f64;
    if calculated_loss_rate > quiche_client.loss_rate + 0.001 {
        quiche_client.max_send_burst = quiche_client.max_send_burst / 4 * 3;
        // Minimun bound of 10xMSS.
        quiche_client.max_send_burst = quiche_client.max_send_burst.max(MAX_DATAGRAM_SIZE * 10);
        quiche_client.loss_rate = calculated_loss_rate;
    }

    let max_send_burst = quiche_client
        .conn
        .send_quantum()
        .min(quiche_client.max_send_burst)
        / MAX_DATAGRAM_SIZE
        * MAX_DATAGRAM_SIZE;
    let mut total_write = 0;
    let mut dst_info = None;

    while total_write < max_send_burst {
        let (write, mut send_info) = match quiche_client
            .conn
            .send(&mut buf[total_write..max_send_burst])
        {
            Ok(v) => v,
            Err(quiche::Error::Done) => {
                trace!("{} done writing", quiche_client.conn.trace_id());
                break;
            }
            Err(e) => {
                log::error!("{} send failed: {:?}", quiche_client.conn.trace_id(), e);
                quiche_client.conn.close(false, 0x1, b"fail").ok();
                break;
            }
        };

        total_write += write;

        // Use the first packet time to send, not the last.
        send_info.to = quiche_client.server_address;
        let _ = dst_info.get_or_insert(send_info);

        if write < MAX_DATAGRAM_SIZE {
            *continue_write = true;
            break;
        }
    }

    if total_write == 0 || dst_info.is_none() {
        return Ok(0);
    }

    if enable_pacing {
        log::debug!("sending with pacing {}", total_write);
        send_with_pacing(
            socket,
            &buf[..total_write],
            &dst_info.unwrap(),
            enable_gso,
            MAX_DATAGRAM_SIZE as u16,
        )
    } else {
        log::debug!("sending without pacing {}", total_write);
        socket.send(&buf[..total_write])
    }
}

/// Handles newly writable streams.
fn handle_writable(
    quiche_client: &mut QuicheClient,
    stream_id: u64,
) -> std::result::Result<(), quiche::Error> {
    if let Some(stream_data) = quiche_client.stream_sender_map.get_mut(&stream_id) {
        let written = stream_data.written;
        if written >= stream_data.data.len() {
            quiche_client.stream_sender_map.remove(&stream_id);
            quiche_client.streams_open = quiche_client.streams_open.saturating_sub(1);
            return Ok(());
        }
        match quiche_client
            .conn
            .stream_send(stream_id, &stream_data.data[written..], true)
        {
            Ok(written) => {
                if written > 0 {
                    stream_data.written += written;
                }
            }
            Err(quiche::Error::Done) => {
                quiche_client.streams_open = quiche_client.streams_open.saturating_sub(1);
                return Ok(());
            }
            Err(e) => {
                quiche_client.streams_open = quiche_client.streams_open.saturating_sub(1);
                log::error!(
                    "{} stream id :{stream_id} send failed {e:?}",
                    quiche_client.conn.trace_id()
                );
                return Err(e);
            }
        }
    } else {
        //close stream
        log::error!(
            "{} stream id :{stream_id} not found",
            quiche_client.conn.trace_id()
        );
        quiche_client
            .conn
            .stream_shutdown(stream_id, quiche::Shutdown::Write, 0)?;
    }
    Ok(())
}

pub fn quiche_transaction_sending_loop(
    number_of_unistreams: u64,
    mut message_channel: mio_channel::Receiver<QuicheMessage>,
    certificate: rustls::Certificate,
    key: rustls::PrivateKey,
    prioritization_heap_size: Option<usize>,
) -> anyhow::Result<()> {
    let socket_addr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0);
    let mut socket = mio::net::UdpSocket::bind(socket_addr)?;

    let mut config = configure_client(certificate, key, number_of_unistreams)?;

    let enable_pacing = set_txtime_sockopt(&socket).is_ok();

    let enable_gso = detect_gso(&socket, MAX_DATAGRAM_SIZE);
    let local_address = socket.local_addr()?;

    let mut poll = mio::Poll::new()?;
    let mut events = mio::Events::with_capacity(1024);
    let mut buf = [0; 65535];

    // Generate a random source connection ID for the connection.
    let rng = SystemRandom::new();

    poll.registry()
        .register(&mut socket, mio::Token(0), mio::Interest::READABLE)?;

    poll.registry()
        .register(&mut message_channel, mio::Token(1), mio::Interest::READABLE)?;

    let mut tpu_clients = BTreeMap::<SocketAddr, QuicheClient>::new();
    let mut continue_write = false;

    loop {
        let timeout = match continue_write {
            true => Some(std::time::Duration::from_secs(0)),
            false => {
                if tpu_clients.is_empty() {
                    Some(std::time::Duration::from_millis(100))
                } else {
                    tpu_clients
                        .iter()
                        .filter_map(|(_, c)| c.conn.timeout())
                        .min()
                }
            }
        };

        poll.poll(&mut events, timeout).unwrap();

        if events.iter().any(|e| e.token() == mio::Token(1)) {
            while let Ok(message) = message_channel.try_recv() {
                match message {
                    QuicheMessage::ResetConnections(addresses) => {
                        let mut to_remove: BTreeSet<SocketAddr> =
                            tpu_clients.keys().cloned().collect();
                        for address in addresses {
                            if let std::collections::btree_map::Entry::Vacant(e) =
                                tpu_clients.entry(address)
                            {
                                log::debug!("creating connection for {}", address);
                                continue_write = true;
                                let Ok(mut quiche_client) = new_quiche_connection(
                                    &socket,
                                    address,
                                    local_address,
                                    &mut config,
                                    prioritization_heap_size,
                                    &rng,
                                ) else {
                                    log::error!("Error creating new connection");
                                    continue;
                                };
                                if let Err(e) = quiche_send_socket(
                                    &mut buf,
                                    &socket,
                                    &mut quiche_client,
                                    enable_pacing,
                                    enable_gso,
                                    &mut continue_write,
                                ) {
                                    log::error!("Error sending socket : {e}");
                                }

                                e.insert(quiche_client);
                            } else {
                                to_remove.remove(&address);
                            }
                        }

                        for address in to_remove {
                            if let Some(mut client) = tpu_clients.remove(&address) {
                                log::debug!("removing connection for {}", address);
                                if let Err(e) = client.conn.close(true, 0, b"resetting connection")
                                {
                                    log::error!("Error closing connection : {e}");
                                }
                                if let Err(e) = quiche_send_socket(
                                    &mut buf,
                                    &socket,
                                    &mut client,
                                    enable_pacing,
                                    enable_gso,
                                    &mut continue_write,
                                ) {
                                    log::error!("Error sending socket : {e}");
                                }
                            }
                        }
                    }
                    QuicheMessage::SendTransaction(transaction) => {
                        log::debug!("recieved transaction {:?}", transaction.signature);
                        for (_, quiche_client) in tpu_clients.iter_mut() {
                            quiche_client
                                .prioritization_fee_heap
                                .insert(transaction.clone());
                            continue_write = true;
                        }
                    }
                    QuicheMessage::UpdateCurrentBlockheight(current_blockheight) => {
                        for (_, quiche_client) in tpu_clients.iter_mut() {
                            quiche_client
                                .prioritization_fee_heap
                                .remove_expired_transactions(current_blockheight);
                        }
                    }
                }
            }
        } else if continue_write || events.iter().any(|e| e.token() == mio::Token(0)) {
            log::debug!("reading from socket");
            // read loop
            // Read incoming UDP packets from the socket and feed them to quiche,
            // until there are no more packets to read.
            let mut updated_sockets = BTreeSet::new();

            if continue_write {
                updated_sockets.extend(tpu_clients.keys());
                continue_write = false;
            }

            'read: loop {
                let (len, from) = match socket.recv_from(&mut buf) {
                    Ok(v) => v,

                    Err(e) => {
                        // There are no more UDP packets to read, so end the read
                        // loop.
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            debug!("recv() would block");
                            break 'read;
                        }

                        log::error!("recv() failed: {:?}", e);
                        break;
                    }
                };

                trace!("got {} bytes", len);

                let recv_info = quiche::RecvInfo {
                    to: socket.local_addr().unwrap(),
                    from,
                };

                updated_sockets.insert(from);
                let Some(quiche_client) = tpu_clients.get_mut(&from) else {
                    continue;
                };

                // Process potentially coalesced packets.
                let read = match quiche_client.conn.recv(&mut buf[..len], recv_info) {
                    Ok(v) => v,

                    Err(e) => {
                        error!("recv failed: {:?}", e);
                        continue 'read;
                    }
                };

                debug!("processed {} bytes", read);
            }

            debug!("done reading");
            for socket_addr in updated_sockets {
                log::debug!("processing socket : {socket_addr}");
                let quiche_client = tpu_clients.get_mut(&socket_addr).unwrap();
                if !quiche_client.has_connected && quiche_client.conn.is_established() {
                    log::debug!("connection established");
                    quiche_client.has_connected = true;
                }
                // See whether source Connection IDs have been retired.
                while let Some(retired_scid) = quiche_client.conn.retired_scid_next() {
                    log::debug!("Retiring source CID {:?}", retired_scid);
                }

                // Provides as many CIDs as possible.
                // while quiche_client.conn.scids_left() > 0 {
                //    let (scid, reset_token) = generate_cid_and_reset_token(&rng);
                //    if conn.new_scid(&scid, reset_token, false).is_err() {
                //        break;
                //    }
                //}

                if quiche_client.conn.is_established() {
                    log::debug!("connection established");
                    // Process all readable streams.
                    for s in quiche_client.conn.readable() {
                        log::info!("got a reable stream : {s} bizzarely");
                    }

                    let writable_streams: Vec<u64> = quiche_client.conn.writable().collect();
                    for s in writable_streams {
                        if let Err(e) = handle_writable(quiche_client, s) {
                            log::error!("Error handling writable stream : {e:?}");
                        }
                    }

                    // send existing transactions in the queue
                    while quiche_client.streams_open < number_of_unistreams as usize {
                        if let Some(transaction) = quiche_client.prioritization_fee_heap.pop() {
                            log::debug!("sending transaction");
                            let next_unidi =
                                get_next_unidi(quiche_client.last_stream_id, false, u64::MAX);
                            let (save_in_stream_sender_map, written) = match quiche_client
                                .conn
                                .stream_send(next_unidi, &transaction.transaction, true)
                            {
                                Ok(written) => {
                                    if written > 0 {
                                        if written < transaction.transaction.len() {
                                            (true, written)
                                        } else {
                                            (false, 0)
                                        }
                                    } else {
                                        (true, 0)
                                    }
                                }
                                Err(_) => {
                                    //retry later
                                    quiche_client.prioritization_fee_heap.insert(transaction);
                                    break;
                                }
                            };

                            if save_in_stream_sender_map {
                                let stream_data = StreamData {
                                    data: transaction.transaction,
                                    written,
                                };
                                quiche_client
                                    .stream_sender_map
                                    .insert(next_unidi, stream_data);
                            }
                            quiche_client.streams_open += 1;
                            quiche_client.last_stream_id = next_unidi;
                        } else {
                            break;
                        }
                    }
                }

                let send_result = quiche_send_socket(
                    &mut buf,
                    &socket,
                    quiche_client,
                    enable_pacing,
                    enable_gso,
                    &mut continue_write,
                );

                if let Err(e) = send_result {
                    log::error!("sending failed with error : {e:?}");
                    break;
                }

                if quiche_client.conn.is_closed() {
                    debug!("connection closed, {:?}", quiche_client.conn.stats());
                    // create a new connection
                    tpu_clients.insert(
                        socket_addr,
                        new_quiche_connection(
                            &socket,
                            socket_addr,
                            local_address,
                            &mut config,
                            prioritization_heap_size,
                            &rng,
                        )?,
                    );
                    break;
                }
            }
        } else {
            for client in &mut tpu_clients.values_mut() {
                client.conn.on_timeout();
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}

#[cfg(test)]
mod tests {
    use solana_client::rpc_client::SerializableTransaction;
    use solana_lite_rpc_core::structures::transaction_sent_info::SentTransactionInfo;
    use solana_sdk::hash::Hash;
    use solana_sdk::instruction::Instruction;
    use solana_sdk::message::Message;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signer::keypair::Keypair;
    use solana_sdk::signer::Signer;
    use solana_sdk::transaction::{Transaction, VersionedTransaction};
    use solana_streamer::nonblocking::quic::{
        compute_max_allowed_uni_streams, SpawnNonBlockingServerResult,
    };
    use solana_streamer::packet::PacketBatch;
    use solana_streamer::quic::StreamStats;
    use solana_streamer::streamer::StakedNodes;
    use solana_streamer::tls_certificates::new_dummy_x509_certificate;
    use std::net::{SocketAddr, UdpSocket};
    use std::str::FromStr;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;
    use tokio::task::JoinHandle;

    use super::{quiche_transaction_sending_loop, QuicheMessage};

    #[allow(dead_code)]
    struct SolanaQuicStreamer {
        sock: UdpSocket,
        exit: Arc<AtomicBool>,
        join_handler: JoinHandle<()>,
        stats: Arc<StreamStats>,
    }

    impl SolanaQuicStreamer {
        fn new_start_listening(
            keypair: Keypair,
            udp_socket: UdpSocket,
            sender: crossbeam_channel::Sender<PacketBatch>,
            staked_nodes: StakedNodes,
        ) -> Self {
            let staked_nodes = Arc::new(RwLock::new(staked_nodes));
            let exit = Arc::new(AtomicBool::new(false));

            let SpawnNonBlockingServerResult { stats, thread, .. } =
                solana_streamer::nonblocking::quic::spawn_server(
                    "test-quic-server",
                    udp_socket.try_clone().unwrap(),
                    &keypair,
                    sender,
                    exit.clone(),
                    1,
                    staked_nodes,
                    10,
                    10,
                    9999, // max_streams_per_ms
                    10,
                    Duration::from_millis(1000),
                    Duration::from_millis(1000),
                )
                .unwrap();

            let addr = udp_socket.local_addr().unwrap().ip();
            let port = udp_socket.local_addr().unwrap().port();
            let _tpu_addr = SocketAddr::new(addr, port);

            Self {
                sock: udp_socket,
                exit,
                join_handler: thread,
                stats,
            }
        }
    }

    const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

    pub fn build_raw_sample_tx(i: u32) -> SentTransactionInfo {
        let payer_keypair = Keypair::from_base58_string(
            "rKiJ7H5UUp3JR18kNyTF1XPuwPKHEM7gMLWHZPWP5djrW1vSjfwjhvJrevxF9MPmUmN9gJMLHZdLMgc9ao78eKr",
        );

        let tx = build_sample_tx(&payer_keypair, i);

        let transaction = Arc::new(
            bincode::serialize::<VersionedTransaction>(&tx).expect("failed to serialize tx"),
        );

        SentTransactionInfo {
            signature: *tx.get_signature(),
            slot: 1,
            transaction,
            last_valid_block_height: 300,
            prioritization_fee: 0,
        }
    }

    fn build_sample_tx(payer_keypair: &Keypair, i: u32) -> VersionedTransaction {
        let blockhash = Hash::default();
        create_memo_tx(format!("hi {}", i).as_bytes(), payer_keypair, blockhash).into()
    }

    fn create_memo_tx(msg: &[u8], payer: &Keypair, blockhash: Hash) -> Transaction {
        let memo = Pubkey::from_str(MEMO_PROGRAM_ID).unwrap();

        let instruction = Instruction::new_with_bytes(memo, msg, vec![]);
        let message = Message::new(&[instruction], Some(&payer.pubkey()));
        Transaction::new(&[payer], message, blockhash)
    }

    #[tokio::test]
    async fn test_quiche_transaction_sending_loop() {
        tracing_subscriber::fmt::init();

        let keypair = Keypair::new();
        let udp_socket_server = UdpSocket::bind("0.0.0.0:0").unwrap();
        let server_socket = udp_socket_server.local_addr().unwrap();
        let (sender, reciever) = crossbeam_channel::unbounded();
        let _quic_stream = SolanaQuicStreamer::new_start_listening(
            keypair,
            udp_socket_server,
            sender,
            StakedNodes::default(),
        );

        let unistream_count = compute_max_allowed_uni_streams(
            solana_streamer::nonblocking::quic::ConnectionPeerType::Unstaked,
            0,
        );

        let (message_sx, message_rx) = mio_channel::channel::<QuicheMessage>();
        let keypair_client = Keypair::new();
        let (certificate, key) = new_dummy_x509_certificate(&keypair_client);
        std::thread::spawn(move || {
            quiche_transaction_sending_loop(
                unistream_count as u64,
                message_rx,
                certificate,
                key,
                None,
            )
            .unwrap();
        });

        std::thread::sleep(Duration::from_millis(10));

        message_sx
            .send(QuicheMessage::ResetConnections(vec![server_socket]))
            .unwrap();

        std::thread::sleep(Duration::from_millis(10));
        let transaction = build_raw_sample_tx(1);
        message_sx
            .send(QuicheMessage::SendTransaction(transaction.clone()))
            .unwrap();

        std::thread::sleep(Duration::from_secs(2));
        let packet_batch = reciever.recv().unwrap();
        let mut packet = packet_batch.iter().next().unwrap().clone();
        assert_eq!(
            &packet.buffer_mut().to_vec(),
            transaction.transaction.as_ref()
        );
    }
}
