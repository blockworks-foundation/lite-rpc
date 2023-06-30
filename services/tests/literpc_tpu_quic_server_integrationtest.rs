use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::ops::Deref;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use crossbeam_channel::Receiver;
use futures::future::join_all;
use log::{debug, info};
use quinn::TokioRuntime;
use solana_rpc_client::rpc_client::SerializableTransaction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use solana_sdk::signer::keypair;
use solana_sdk::transaction::{Transaction, VersionedTransaction};
use solana_streamer::nonblocking::quic::ConnectionPeerType;
use solana_streamer::packet::PacketBatch;
use solana_streamer::quic::StreamStats;
use solana_streamer::streamer::StakedNodes;
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use tokio::runtime::{Builder, Runtime};
use tokio::{join, spawn};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::SendError;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing_subscriber::util::SubscriberInitExt;
use solana_lite_rpc_core::structures::identity_stakes::IdentityStakes;
use solana_lite_rpc_core::tx_store::empty_tx_store;
use solana_lite_rpc_services::tpu_utils::tpu_connection_manager::TpuConnectionManager;

const MAXIMUM_TRANSACTIONS_IN_QUEUE: usize = 200_000;

#[test]
pub fn wireup_and_send_txs_via_channel() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::TRACE).init();

    // solana quic streamer - see quic.rs -> rt()
    const NUM_QUIC_STREAMER_WORKER_THREADS: usize = 1;
    let runtime1 = Builder::new_multi_thread()
        .worker_threads(NUM_QUIC_STREAMER_WORKER_THREADS)
        .thread_name("quic-server")
        .enable_all()
        .build()
        .expect("failed to build tokio runtime for testing quic server");

    // lite-rpc
    let runtime2 = tokio::runtime::Builder::new_multi_thread()
        // see lite-rpc -> main.rs
        .worker_threads(16)
        .enable_all()
        .build()
        .expect("failed to build tokio runtime for lite-rpc");


    let udp_listen_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let listen_addr = udp_listen_socket.local_addr()?;
    let literpc_validator_identity = Arc::new(Keypair::new());

    runtime1.block_on(async {

        /// setup solana Quic streamer
        // see log "Start quic server on UdpSocket { addr: 127.0.0.1:xxxxx, fd: 10 }"

        let mut solana_quic_streamer = SolanaQuicStreamer::new_start_listening(udp_listen_socket);

        if STAKE_CONNECTION {
            solana_quic_streamer.add_stake_for_identity(
                literpc_validator_identity.as_ref(),
                30,
            );
        }


    });

    runtime2.block_on(start_literpc_client(listen_addr, literpc_validator_identity))?;

    // shutdown streamer
    // solana_quic_streamer.shutdown().await;

    Ok(())
}

const STAKE_CONNECTION: bool = true;

async fn start_literpc_client(listen_addrs: SocketAddr, literpc_validator_identity: Arc<Keypair>) -> anyhow::Result<()> {
    let fanout_slots = 4;

    // (String, Vec<u8>) (signature, transaction)
    // _keeper is used to prevent the ref-count base closing to kick in
    let (sender, _keeper) = tokio::sync::broadcast::channel(MAXIMUM_TRANSACTIONS_IN_QUEUE);
    let broadcast_sender = Arc::new(sender);
    let (certificate, key) = new_self_signed_tls_certificate(
        literpc_validator_identity.as_ref(),
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
    )
        .expect("Failed to initialize QUIC connection certificates");

    let tpu_connection_manager =
        TpuConnectionManager::new(certificate, key, fanout_slots as usize);

    // this effectively controls how many connections we will have
    let mut connections_to_keep: HashMap<Pubkey, SocketAddr> = HashMap::new();
    connections_to_keep.insert(
        Pubkey::from_str("1111111jepwNWbYG87sgwnBbUJnQHrPiUJzMpqJXZ")?,
        "127.0.0.1:20001".parse()?,
    );

    connections_to_keep.insert(
        Pubkey::from_str("1111111k4AYMctpyJakWNvGcte6tR8BLyZw54R8qu")?,
        "127.0.0.1:20002".parse()?,
    );

    // this is the real streamer
    connections_to_keep.insert(
        literpc_validator_identity.pubkey(),
        listen_addrs,
    );

    // get information about the optional validator identity stake
    // populated from get_stakes_for_identity()
    let identity_stakes = IdentityStakes {
        peer_type: ConnectionPeerType::Staked,
        stakes: if STAKE_CONNECTION { 30 } else { 0 }, // stake of lite-rpc
        min_stakes: 0,
        max_stakes: 40,
        total_stakes: 100,
    };

    // solana_streamer::nonblocking::quic: Peer type: Staked, stake 30, total stake 0, max streams 128 receive_window Ok(12320) from peer 127.0.0.1:8000

    tpu_connection_manager
        .update_connections(
            broadcast_sender.clone(),
            connections_to_keep,
            identity_stakes,
            // note: tx_store is useless in this scenario as it is never changed; it's only used to check for duplicates
            empty_tx_store().clone(),
        )
        .await;

    let raw_sample_tx = build_raw_sample_tx();

    broadcast_sender.send(raw_sample_tx)?;

    // TODO improve
    sleep(Duration::from_millis(100)).await;

    debug!("remaining: {}", broadcast_sender.len());
    Ok(())
}

#[tokio::test]
// taken from solana -> test_nonblocking_quic_client_multiple_writes
async fn solana_quic_streamer_start() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::TRACE).init();

    let (sender, _receiver) = crossbeam_channel::unbounded();
    let staked_nodes = Arc::new(RwLock::new(StakedNodes::default()));
    // will create random free port
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let exit = Arc::new(AtomicBool::new(false));
    // keypair to derive the server tls certificate
    let keypair = Keypair::new();
    // gossip_host is used in the server certificate
    let gossip_host = "127.0.0.1".parse().unwrap();
    let stats = Arc::new(StreamStats::default());
    let (_, t) = solana_streamer::nonblocking::quic::spawn_server(
        sock.try_clone().unwrap(),
        &keypair,
        gossip_host,
        sender,
        exit.clone(),
        1,
        staked_nodes,
        10,
        10,
        stats.clone(),
        1000,
    )
        .unwrap();

    let addr = sock.local_addr().unwrap().ip();
    let port = sock.local_addr().unwrap().port();
    let tpu_addr = SocketAddr::new(addr, port);

    sleep(Duration::from_millis(500)).await;

    exit.store(true, Ordering::Relaxed);
    t.await.unwrap();

    stats.report();
}

struct SolanaQuicStreamer {
    sock: UdpSocket,
    exit: Arc<AtomicBool>,
    join_handler: JoinHandle<()>,
    stats: Arc<StreamStats>,
    staked_nodes: Arc<RwLock<StakedNodes>>,
}

impl SolanaQuicStreamer {
    pub fn get_socket_addr(&self) -> SocketAddr {
        self.sock.local_addr().unwrap()
    }
    pub fn add_stake_for_identity(&self, identity: &Keypair, stake: u64) {
        let mut lock = self.staked_nodes.write().unwrap();
        let prev = lock.pubkey_stake_map.insert(identity.pubkey(), stake);
        assert!(prev.is_none(), "identity {} already staked", identity.pubkey());
    }
}

impl SolanaQuicStreamer {
    pub async fn shutdown(self) {
        self.exit.store(true, Ordering::Relaxed);
        self.join_handler.await.unwrap();
        self.stats.report();
    }
}

impl SolanaQuicStreamer {
    fn new_start_listening(udp_socket: UdpSocket) -> Self {
        let (sender, _receiver) = crossbeam_channel::unbounded();
        let staked_nodes = Arc::new(RwLock::new(StakedNodes::default()));
        let exit = Arc::new(AtomicBool::new(false));
        // keypair to derive the server tls certificate
        let keypair = Keypair::new();
        // gossip_host is used in the server certificate
        let gossip_host = "127.0.0.1".parse().unwrap();
        let stats = Arc::new(StreamStats::default());
        let (_, jh) = solana_streamer::nonblocking::quic::spawn_server(
            udp_socket.try_clone().unwrap(),
            &keypair,
            gossip_host,
            sender,
            exit.clone(),
            1,
            staked_nodes.clone(),
            10,
            10,
            stats.clone(),
            1000,
        )
            .unwrap();

        let addr = udp_socket.local_addr().unwrap().ip();
        let port = udp_socket.local_addr().unwrap().port();
        let tpu_addr = SocketAddr::new(addr, port);

        Self {
            sock: udp_socket,
            exit,
            join_handler: jh,
            stats,
            staked_nodes,
        }
    }
}

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";


pub fn build_raw_sample_tx() -> (String, Vec<u8>) {

    // FIXME
    let payer_keypair = keypair::read_keypair_file(
        Path::new("/Users/stefan/mango/solana-wallet/solana-testnet-stefantest.json")
    ).unwrap();


    let tx = build_sample_tx(&payer_keypair);

    let raw_tx = bincode::serialize::<VersionedTransaction>(&tx).expect("failed to serialize tx");

    (tx.get_signature().to_string(), raw_tx)
}

fn build_sample_tx(payer_keypair: &Keypair) -> VersionedTransaction {
    let blockhash = Hash::default();
    create_memo_tx(b"hi", payer_keypair, blockhash).into()
}

fn create_memo_tx(msg: &[u8], payer: &Keypair, blockhash: Hash) -> Transaction {
    let memo = Pubkey::from_str(MEMO_PROGRAM_ID).unwrap();

    let instruction = Instruction::new_with_bytes(memo, msg, vec![]);
    let message = Message::new(&[instruction], Some(&payer.pubkey()));
    Transaction::new(&[payer], message, blockhash)
}

fn check_packets(
    receiver: Receiver<PacketBatch>,
    num_bytes: usize,
    num_expected_packets: usize,
) {
    let mut all_packets = vec![];
    let now = Instant::now();
    let mut total_packets: usize = 0;
    while now.elapsed().as_secs() < 10 {
        if let Ok(packets) = receiver.recv_timeout(Duration::from_secs(1)) {
            total_packets = total_packets.saturating_add(packets.len());
            all_packets.push(packets)
        }
        if total_packets >= num_expected_packets {
            break;
        }
    }
    for batch in all_packets {
        for p in &batch {
            assert_eq!(p.meta().size, num_bytes);
        }
    }
    assert_eq!(total_packets, num_expected_packets);
}
