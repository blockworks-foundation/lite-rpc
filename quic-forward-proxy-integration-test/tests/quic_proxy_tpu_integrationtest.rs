use countmap::CountMap;
use crossbeam_channel::Sender;

use log::{debug, error, info, trace, warn};

use solana_lite_rpc_core::quic_connection_utils::QuicConnectionParameters;
use solana_lite_rpc_core::solana_utils::SerializableTransaction;
use solana_lite_rpc_core::stores::tx_store::empty_tx_store;
use solana_lite_rpc_core::structures::identity_stakes::IdentityStakesData;
use solana_lite_rpc_services::tpu_utils::tpu_connection_manager::TpuConnectionManager;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};

use solana_sdk::transaction::{Transaction, VersionedTransaction};
use solana_streamer::nonblocking::quic::ConnectionPeerType;
use solana_streamer::packet::PacketBatch;
use solana_streamer::quic::StreamStats;
use solana_streamer::streamer::StakedNodes;
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};

use itertools::Itertools;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Builder;

use tokio::task::{yield_now, JoinHandle};
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

use solana_lite_rpc_quic_forward_proxy::proxy::QuicForwardProxy;
use solana_lite_rpc_quic_forward_proxy::tls_self_signed_pair_generator::SelfSignedTlsConfigProvider;
use solana_lite_rpc_quic_forward_proxy::validator_identity::ValidatorIdentity;
use solana_lite_rpc_services::tpu_utils::quic_proxy_connection_manager::QuicProxyConnectionManager;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Copy, Clone, Debug)]
struct TestCaseParams {
    sample_tx_count: u32,
    stake_connection: bool,
    proxy_mode: bool,
}

const MAXIMUM_TRANSACTIONS_IN_QUEUE: usize = 16_384;
const MAX_QUIC_CONNECTIONS_PER_PEER: usize = 8; // like solana repo

const QUIC_CONNECTION_PARAMS: QuicConnectionParameters = QuicConnectionParameters {
    connection_timeout: Duration::from_secs(2),
    connection_retry_count: 10,
    finalize_timeout: Duration::from_secs(2),
    max_number_of_connections: 8,
    unistream_timeout: Duration::from_secs(2),
    write_timeout: Duration::from_secs(2),
    number_of_transactions_per_unistream: 10,
};

#[test]
pub fn small_tx_batch_staked_direct() {
    configure_logging(true);

    wireup_and_send_txs_via_channel(TestCaseParams {
        sample_tx_count: 20,
        stake_connection: true,
        proxy_mode: false,
    });
}

#[test]
pub fn small_tx_batch_staked_proxy() {
    configure_logging(true);

    wireup_and_send_txs_via_channel(TestCaseParams {
        sample_tx_count: 20,
        stake_connection: true,
        proxy_mode: true,
    });
}

#[test]
pub fn small_tx_batch_unstake_direct() {
    configure_logging(true);

    wireup_and_send_txs_via_channel(TestCaseParams {
        sample_tx_count: 20,
        stake_connection: false,
        proxy_mode: false,
    });
}

#[ignore]
#[test]
pub fn with_100_transactions_direct() {
    configure_logging(false);

    wireup_and_send_txs_via_channel(TestCaseParams {
        sample_tx_count: 100,
        stake_connection: true,
        proxy_mode: false,
    });
}

#[ignore]
#[test]
pub fn with_1000_transactions_direct() {
    configure_logging(false);

    wireup_and_send_txs_via_channel(TestCaseParams {
        sample_tx_count: 1000,
        stake_connection: true,
        proxy_mode: false,
    });
}

// note: this tests are flakes on CI ond also local (see https://mangolana.atlassian.net/browse/MAN-59)
#[ignore]
#[test]
pub fn bench_proxy() {
    configure_logging(true);

    // consumed 1000 packets in 2059004us - throughput 485.67 tps, throughput_50 6704.05 tps

    wireup_and_send_txs_via_channel(TestCaseParams {
        sample_tx_count: 1000,
        stake_connection: true,
        proxy_mode: true,
    });
}

// note: this tests are flakes on CI ond also local (see https://mangolana.atlassian.net/browse/MAN-59)
#[ignore]
#[test]
pub fn with_10000_transactions_direct() {
    configure_logging(false);

    wireup_and_send_txs_via_channel(TestCaseParams {
        sample_tx_count: 10000,
        stake_connection: true,
        proxy_mode: false,
    });
}

// note: this tests are flakes on CI ond also local (see https://mangolana.atlassian.net/browse/MAN-59)
#[ignore]
#[test]
pub fn with_10000_transactions_proxy() {
    configure_logging(false);

    wireup_and_send_txs_via_channel(TestCaseParams {
        sample_tx_count: 10000,
        stake_connection: true,
        proxy_mode: true,
    });
}

// note: this tests are flakes on CI ond also local (see https://mangolana.atlassian.net/browse/MAN-59)
#[ignore]
#[test]
pub fn many_transactions_proxy() {
    configure_logging(false);

    wireup_and_send_txs_via_channel(TestCaseParams {
        sample_tx_count: 100000,
        stake_connection: false,
        proxy_mode: true,
    });
}

// note: this not a tokio test as runtimes get created as part of the integration test
fn wireup_and_send_txs_via_channel(test_case_params: TestCaseParams) {
    configure_panic_hook();

    // value from solana - see quic streamer - see quic.rs -> rt()
    let runtime_quic1 = Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("quic-server")
        .enable_all()
        .build()
        .expect("failed to build tokio runtime for testing quic server");

    // lite-rpc
    let runtime_literpc = Builder::new_multi_thread()
        // see lite-rpc -> main.rs
        .worker_threads(16) // also works with 1
        .enable_all()
        .build()
        .expect("failed to build tokio runtime for lite-rpc-tpu-client");

    // quic-forward-proxy
    let runtime_quic_proxy = Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime for quic-forward-proxy");

    let literpc_validator_identity = Arc::new(Keypair::new());
    let udp_listen_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let listen_addr = udp_listen_socket.local_addr().unwrap();

    let proxy_listen_addr = UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();

    let (inbound_packets_sender, inbound_packets_receiver) = crossbeam_channel::unbounded();

    runtime_quic1.block_on(async {
        // setup solana Quic streamer
        // see log "Start quic server on UdpSocket { addr: 127.0.0.1:xxxxx, fd: 10 }"
        let stakes_map = Arc::new(if test_case_params.stake_connection {
            let mut map = HashMap::new();
            map.insert(literpc_validator_identity.pubkey(), 30);
            map
        } else {
            HashMap::default()
        });
        let staked_nodes = StakedNodes::new(stakes_map, Default::default());

        let _solana_quic_streamer = SolanaQuicStreamer::new_start_listening(
            udp_listen_socket,
            inbound_packets_sender,
            staked_nodes,
        );
    });

    runtime_quic_proxy.block_on(async {
        tokio::spawn(start_quic_proxy(proxy_listen_addr));
    });

    runtime_literpc.block_on(async {
        if test_case_params.proxy_mode {
            tokio::spawn(start_literpc_client_proxy_mode(
                test_case_params,
                listen_addr,
                literpc_validator_identity,
                proxy_listen_addr,
            ));
        } else {
            tokio::spawn(start_literpc_client_direct_mode(
                test_case_params,
                listen_addr,
                literpc_validator_identity,
            ));
        }
    });

    let packet_consumer_jh = thread::spawn(move || {
        info!("start pulling packets...");
        let mut packet_count = 0;
        let time_to_first = Instant::now();
        let mut latest_tx = Instant::now();
        let timer = Instant::now();
        // second half
        let mut timer2 = None;
        let mut packet_count2 = 0;
        let mut count_map: CountMap<Signature> =
            CountMap::with_capacity(test_case_params.sample_tx_count as usize);
        let mut contents: HashSet<String> =
            HashSet::with_capacity(test_case_params.sample_tx_count as usize);
        let warmup_tx_count: u32 = test_case_params.sample_tx_count / 2;

        while (count_map.len() as u32) < test_case_params.sample_tx_count {
            if latest_tx.elapsed() > Duration::from_secs(50) {
                warn!("abort after timeout waiting for packet from quic streamer");
                break;
            }

            let packet_batch = match inbound_packets_receiver
                .recv_timeout(Duration::from_millis(500))
            {
                Ok(batch) => batch,
                Err(_) => {
                    debug!("consumer thread did not receive packets on inbound channel recently - continue polling");
                    continue;
                }
            };

            // reset timer
            latest_tx = Instant::now();

            if packet_count == 0 {
                info!(
                    "time to first packet {}ms",
                    time_to_first.elapsed().as_millis()
                );
            }

            packet_count += packet_batch.len() as u32;
            if timer2.is_some() {
                packet_count2 += packet_batch.len() as u32;
            }

            for packet in packet_batch.iter() {
                let tx = packet
                    .deserialize_slice::<VersionedTransaction, _>(..)
                    .unwrap();
                trace!(
                    "read transaction from quic streaming server: {:?}",
                    tx.get_signature()
                );
                // "hi 7292   "
                let content = String::from_utf8(tx.message.instructions()[0].data.clone()).unwrap();
                contents.insert(content);

                count_map.insert_or_increment(*tx.get_signature());
            }

            if packet_count == warmup_tx_count {
                timer2 = Some(Instant::now());
            }
        } // -- while not all packets received - by count

        let total_duration = timer.elapsed();
        let half_duration = timer2
            .map(|t| t.elapsed())
            .unwrap_or(Duration::from_secs(3333));

        // throughput_50 is second half of transactions - should iron out startup effects
        info!(
            "consumed {} packets in {}us - throughput {:.2} tps, throughput_50 {:.2} tps, ",
            packet_count,
            total_duration.as_micros(),
            packet_count as f64 / total_duration.as_secs_f64(),
            packet_count2 as f64 / half_duration.as_secs_f64(),
        );

        info!("got all expected packets - shutting down tokio runtime with lite-rpc client");

        // inspect content and find missing
        let all_numbers: HashSet<u64> = contents
            .iter()
            .map(|content| parse_hi(content).unwrap())
            .sorted()
            .collect();
        let max_number = *all_numbers.iter().max().unwrap();
        // OKEY: got 100000 distinct content strings with max 99999
        info!(
            "got {} distinct content strings with max {}",
            contents.len(),
            max_number
        );
        let missing = (0..=max_number)
            .filter(|n| !all_numbers.contains(n))
            .collect_vec();
        for mi in missing {
            info!("- missing {}", mi);
        }

        assert_eq!(
            count_map.len() as u32,
            test_case_params.sample_tx_count,
            "count_map size should be equal to sample_tx_count"
        );
        // note: this assumption will not hold as soon as test is configured to do fanout
        assert!(
            count_map.values().all(|cnt| *cnt >= 1),
            "all transactions should be unique"
        );

        runtime_literpc.shutdown_timeout(Duration::from_millis(1000));
    });

    packet_consumer_jh.join().unwrap();
}

fn configure_panic_hook() {
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        default_panic(panic_info);
        if let Some(location) = panic_info.location() {
            error!(
                "panic occurred in file '{}' at line {}",
                location.file(),
                location.line(),
            );
        } else {
            error!("panic occurred but can't get location information...");
        }
        // note: we do not exit the process to allow proper test execution
    }));
}

fn configure_logging(verbose: bool) {
    let env_filter = if verbose {
        "debug,rustls=info,quinn=info,quinn_proto=debug,solana_streamer=debug,solana_lite_rpc_quic_forward_proxy=trace"
    } else {
        "info,rustls=info,quinn=info,quinn_proto=info,solana_streamer=info,solana_lite_rpc_quic_forward_proxy=info"
    };
    let span_mode = if verbose {
        FmtSpan::CLOSE
    } else {
        FmtSpan::NONE
    };
    // EnvFilter::try_from_default_env().unwrap_or(env_filter)
    let filter =
        EnvFilter::try_from_default_env().unwrap_or(EnvFilter::from_str(env_filter).unwrap());

    let result = tracing_subscriber::fmt::fmt()
        .with_env_filter(filter)
        .with_span_events(span_mode)
        .try_init();
    if result.is_err() {
        println!("Logging already initialized - ignore");
    }
}

#[tokio::test]
// taken from solana -> test_nonblocking_quic_client_multiple_writes
async fn solana_quic_streamer_start() {
    let (sender, _receiver) = crossbeam_channel::unbounded();
    let staked_nodes = Arc::new(RwLock::new(StakedNodes::default()));
    // will create random free port
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let exit = Arc::new(AtomicBool::new(false));
    // keypair to derive the server tls certificate
    let keypair = Keypair::new();
    // gossip_host is used in the server certificate
    let gossip_host = "127.0.0.1".parse().unwrap();
    let (_, stats, t) = solana_streamer::nonblocking::quic::spawn_server(
        "test-quic-server",
        sock.try_clone().unwrap(),
        &keypair,
        gossip_host,
        sender,
        exit.clone(),
        1,
        staked_nodes,
        10,
        10,
        Duration::from_millis(1000),
        Duration::from_millis(1000),
    )
    .unwrap();

    let addr = sock.local_addr().unwrap().ip();
    let port = sock.local_addr().unwrap().port();
    let _tpu_addr = SocketAddr::new(addr, port);

    // sleep(Duration::from_millis(500)).await;

    exit.store(true, Ordering::Relaxed);
    t.await.unwrap();

    stats.report("test-quic-streamer");
}

// no quic proxy
async fn start_literpc_client_direct_mode(
    test_case_params: TestCaseParams,
    streamer_listen_addrs: SocketAddr,
    literpc_validator_identity: Arc<Keypair>,
) -> anyhow::Result<()> {
    info!("Start lite-rpc test client in direct-mode...");

    let fanout_slots = 4;

    // (String, Vec<u8>) (signature, transaction)
    let (sender, _) = tokio::sync::broadcast::channel(MAXIMUM_TRANSACTIONS_IN_QUEUE);
    let broadcast_sender = Arc::new(sender);
    let (certificate, key) = new_self_signed_tls_certificate(
        literpc_validator_identity.as_ref(),
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
    )
    .expect("Failed to initialize QUIC connection certificates");

    let tpu_connection_manager =
        TpuConnectionManager::new(certificate, key, fanout_slots as usize).await;

    // this effectively controls how many connections we will have
    let mut connections_to_keep: HashMap<Pubkey, SocketAddr> = HashMap::new();
    let addr1 = UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    connections_to_keep.insert(
        Pubkey::from_str("1111111jepwNWbYG87sgwnBbUJnQHrPiUJzMpqJXZ")?,
        addr1,
    );

    let addr2 = UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    connections_to_keep.insert(
        Pubkey::from_str("1111111k4AYMctpyJakWNvGcte6tR8BLyZw54R8qu")?,
        addr2,
    );

    // this is the real streamer
    connections_to_keep.insert(literpc_validator_identity.pubkey(), streamer_listen_addrs);

    // get information about the optional validator identity stake
    // populated from get_stakes_for_identity()
    let identity_stakes = IdentityStakesData {
        peer_type: ConnectionPeerType::Staked,
        stakes: if test_case_params.stake_connection {
            30
        } else {
            0
        }, // stake of lite-rpc
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
            QUIC_CONNECTION_PARAMS,
        )
        .await;

    for i in 0..test_case_params.sample_tx_count {
        let raw_sample_tx = build_raw_sample_tx(i);
        trace!(
            "broadcast transaction {} to {} receivers: {}",
            raw_sample_tx.0,
            broadcast_sender.receiver_count(),
            format!("hi {}", i)
        );

        broadcast_sender.send(raw_sample_tx)?;
    }

    while !broadcast_sender.is_empty() {
        sleep(Duration::from_millis(1000)).await;
        warn!("broadcast channel is not empty - wait before shutdown test client thread");
    }

    assert!(
        broadcast_sender.is_empty(),
        "broadcast channel must be empty"
    );

    sleep(Duration::from_secs(3)).await;

    Ok(())
}

async fn start_literpc_client_proxy_mode(
    test_case_params: TestCaseParams,
    streamer_listen_addrs: SocketAddr,
    validator_identity: Arc<Keypair>,
    forward_proxy_address: SocketAddr,
) -> anyhow::Result<()> {
    info!(
        "Start lite-rpc test client using quic proxy at {} ...",
        forward_proxy_address
    );

    // (String, Vec<u8>) (signature, transaction)
    let (sender, _) = tokio::sync::broadcast::channel(MAXIMUM_TRANSACTIONS_IN_QUEUE);
    let broadcast_sender = Arc::new(sender);
    let (certificate, key) = new_self_signed_tls_certificate(
        validator_identity.as_ref(),
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
    )
    .expect("Failed to initialize QUIC connection certificates");

    let quic_proxy_connection_manager =
        QuicProxyConnectionManager::new(certificate, key, forward_proxy_address).await;

    // this effectively controls how many connections we will have
    let mut connections_to_keep: HashMap<Pubkey, SocketAddr> = HashMap::new();
    let addr1 = UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    connections_to_keep.insert(
        Pubkey::from_str("1111111jepwNWbYG87sgwnBbUJnQHrPiUJzMpqJXZ")?,
        addr1,
    );

    let addr2 = UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    connections_to_keep.insert(
        Pubkey::from_str("1111111k4AYMctpyJakWNvGcte6tR8BLyZw54R8qu")?,
        addr2,
    );

    // this is the real streamer
    connections_to_keep.insert(validator_identity.pubkey(), streamer_listen_addrs);

    // get information about the optional validator identity stake
    // populated from get_stakes_for_identity()
    let _identity_stakes = IdentityStakesData {
        peer_type: ConnectionPeerType::Staked,
        stakes: if test_case_params.stake_connection {
            30
        } else {
            0
        }, // stake of lite-rpc
        min_stakes: 0,
        max_stakes: 40,
        total_stakes: 100,
    };

    // solana_streamer::nonblocking::quic: Peer type: Staked, stake 30, total stake 0, max streams 128 receive_window Ok(12320) from peer 127.0.0.1:8000
    //
    // tpu_connection_manager
    //     .update_connections(
    //         broadcast_sender.clone(),
    //         connections_to_keep,
    //         identity_stakes,
    //         // note: tx_store is useless in this scenario as it is never changed; it's only used to check for duplicates
    //         empty_tx_store().clone(),
    //         QUIC_CONNECTION_PARAMS,
    //     )
    //     .await;

    let transaction_receiver = broadcast_sender.subscribe();
    quic_proxy_connection_manager
        .update_connection(
            transaction_receiver,
            connections_to_keep,
            QUIC_CONNECTION_PARAMS,
        )
        .await;

    for i in 0..test_case_params.sample_tx_count {
        let raw_sample_tx = build_raw_sample_tx(i);
        trace!(
            "broadcast transaction {} to {} receivers: {}",
            raw_sample_tx.0,
            broadcast_sender.receiver_count(),
            format!("hi {}", i)
        );

        broadcast_sender.send(raw_sample_tx)?;
        if (i + 1) % 1000 == 0 {
            yield_now().await;
        }
    }

    while !broadcast_sender.is_empty() {
        sleep(Duration::from_millis(1000)).await;
        warn!("broadcast channel is not empty - wait before shutdown test client thread");
    }

    assert!(
        broadcast_sender.is_empty(),
        "broadcast channel must be empty"
    );

    quic_proxy_connection_manager.signal_shutdown();

    sleep(Duration::from_secs(3)).await;

    Ok(())
}

async fn start_quic_proxy(proxy_listen_addr: SocketAddr) -> anyhow::Result<()> {
    let _tls_configuration = SelfSignedTlsConfigProvider::new_singleton_self_signed_localhost();
    let random_unstaked_validator_identity = ValidatorIdentity::new(None);

    let tls_config = Arc::new(SelfSignedTlsConfigProvider::new_singleton_self_signed_localhost());
    let proxy_service = QuicForwardProxy::new(
        proxy_listen_addr,
        tls_config,
        random_unstaked_validator_identity,
    )
    .await?
    .start_services();

    tokio::select! {
        res = proxy_service => {
            panic!("Proxy service stopped unexpectedly: {:?}", res)
        },
    }
}

#[allow(dead_code)]
struct SolanaQuicStreamer {
    sock: UdpSocket,
    exit: Arc<AtomicBool>,
    join_handler: JoinHandle<()>,
    stats: Arc<StreamStats>,
}

impl SolanaQuicStreamer {
    fn new_start_listening(
        udp_socket: UdpSocket,
        sender: Sender<PacketBatch>,
        staked_nodes: StakedNodes,
    ) -> Self {
        let staked_nodes = Arc::new(RwLock::new(staked_nodes));
        let exit = Arc::new(AtomicBool::new(false));
        // keypair to derive the server tls certificate
        let keypair = Keypair::new();
        // gossip_host is used in the server certificate
        let gossip_host = "127.0.0.1".parse().unwrap();
        let (_, stats, jh) = solana_streamer::nonblocking::quic::spawn_server(
            "test-quic-server",
            udp_socket.try_clone().unwrap(),
            &keypair,
            gossip_host,
            sender,
            exit.clone(),
            MAX_QUIC_CONNECTIONS_PER_PEER,
            staked_nodes,
            10,
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
            join_handler: jh,
            stats,
        }
    }
}

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

pub fn build_raw_sample_tx(i: u32) -> (String, Vec<u8>) {
    let payer_keypair = Keypair::from_base58_string(
        "rKiJ7H5UUp3JR18kNyTF1XPuwPKHEM7gMLWHZPWP5djrW1vSjfwjhvJrevxF9MPmUmN9gJMLHZdLMgc9ao78eKr",
    );

    let tx = build_sample_tx(&payer_keypair, i);

    let raw_tx = bincode::serialize::<VersionedTransaction>(&tx).expect("failed to serialize tx");

    (tx.get_signature().to_string(), raw_tx)
}

// "hi 1234 " -> 1234
fn parse_hi(input: &str) -> Option<u64> {
    let input = input.trim();
    let input = input.replace("hi ", "");
    input.parse::<u64>().ok()
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
