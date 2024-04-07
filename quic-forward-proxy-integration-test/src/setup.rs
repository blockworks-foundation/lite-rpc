use log::{debug, error, info, trace, warn};

use solana_lite_rpc_core::solana_utils::SerializableTransaction;
use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_lite_rpc_core::structures::identity_stakes::IdentityStakesData;
use solana_lite_rpc_core::structures::transaction_sent_info::SentTransactionInfo;
use solana_lite_rpc_services::quic_connection_utils::QuicConnectionParameters;
use solana_lite_rpc_services::tpu_utils::tpu_connection_manager::TpuConnectionManager;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::Message;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use solana_sdk::hash::Hash;

use solana_sdk::transaction::{Transaction, VersionedTransaction};
use solana_streamer::nonblocking::quic::ConnectionPeerType;
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::{yield_now, JoinHandle};
use tokio::time::sleep;
use solana_lite_rpc_services::tpu_utils::quic_proxy_connection_manager::QuicProxyConnectionManager;

pub const MAXIMUM_TRANSACTIONS_IN_QUEUE: usize = 16_384;
pub const MAX_QUIC_CONNECTIONS_PER_PEER: usize = 8; // like solana repo

pub const QUIC_CONNECTION_PARAMS: QuicConnectionParameters = QuicConnectionParameters {
    connection_timeout: Duration::from_secs(2),
    connection_retry_count: 10,
    finalize_timeout: Duration::from_secs(2),
    max_number_of_connections: 8,
    unistream_timeout: Duration::from_secs(2),
    write_timeout: Duration::from_secs(2),
    number_of_transactions_per_unistream: 10,
    unistreams_to_create_new_connection_in_percentage: 10,
    prioritization_heap_size: None,
};

#[derive(Copy, Clone, Debug)]
pub struct TestCaseParams {
    pub sample_tx_count: u32,
    pub stake_connection: bool,
    pub proxy_mode: bool,
}

pub async fn start_literpc_client_proxy_mode(
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
            raw_sample_tx.signature,
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


// no quic proxy
pub async fn start_literpc_client_direct_mode(
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
            DataCache::new_for_tests(),
            QUIC_CONNECTION_PARAMS,
        )
        .await;

    for i in 0..test_case_params.sample_tx_count {
        let raw_sample_tx = build_raw_sample_tx(i);
        trace!(
            "broadcast transaction {} to {} receivers: {}",
            raw_sample_tx.signature,
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

const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

pub fn build_raw_sample_tx(i: u32) -> SentTransactionInfo {
    let payer_keypair = Keypair::from_base58_string(
        "rKiJ7H5UUp3JR18kNyTF1XPuwPKHEM7gMLWHZPWP5djrW1vSjfwjhvJrevxF9MPmUmN9gJMLHZdLMgc9ao78eKr",
    );

    let tx = build_sample_tx(&payer_keypair, i);

    let transaction =
        Arc::new(bincode::serialize::<VersionedTransaction>(&tx).expect("failed to serialize tx"));

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