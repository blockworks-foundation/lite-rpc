use crate::outbound::debouncer::Debouncer;
use crate::outbound::sharder::Sharder;
use crate::quic_util::SkipServerVerification;
use crate::quinn_auto_reconnect::AutoReconnect;
use crate::shared::ForwardPacket;
use crate::util::timeout_fallback;
use crate::validator_identity::ValidatorIdentity;
use anyhow::{bail, Context};
use futures::future::join_all;
use log::{debug, info, trace, warn};
use quinn::{
    ClientConfig, Endpoint, EndpointConfig, IdleTimeout, TokioRuntime, TransportConfig, VarInt,
};
use solana_sdk::quic::QUIC_MAX_TIMEOUT;
use solana_streamer::nonblocking::quic::{ALPN_TPU_PROTOCOL_ID, ConnectionPeerType};
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::VersionedTransaction;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;
use solana_lite_rpc_core::quic_connection_utils::QuicConnectionParameters;
use solana_lite_rpc_core::solana_utils::SerializableTransaction;
use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_lite_rpc_core::structures::identity_stakes::IdentityStakesData;
use solana_lite_rpc_core::structures::transaction_sent_info::SentTransactionInfo;
use crate::outbound::tpu_connection_manager::TpuConnectionManager;


const MAXIMUM_TRANSACTIONS_IN_QUEUE: usize = 16_384;


const QUIC_CONNECTION_PARAMS: QuicConnectionParameters = QuicConnectionParameters {
    connection_timeout: Duration::from_secs(2),
    connection_retry_count: 10,
    finalize_timeout: Duration::from_secs(2),
    max_number_of_connections: 8,
    unistream_timeout: Duration::from_secs(2),
    write_timeout: Duration::from_secs(2),
    number_of_transactions_per_unistream: 10,
};


pub async fn ng_forwarder(
    validator_identity: ValidatorIdentity,
    mut transaction_channel: Receiver<ForwardPacket>,
    exit_signal: Arc<AtomicBool>,
) -> anyhow::Result<()> {

    // TODO
    let fanout_slots = 4;

    let (certificate, key) = new_self_signed_tls_certificate(
        &validator_identity.get_keypair_for_tls(),
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
    )
        .expect("Failed to initialize QUIC connection certificates");

    // TODO make copy of TpuConnectionManager in proxy crate an strip unused features
    let tpu_connection_manager =
        TpuConnectionManager::new(certificate, key, fanout_slots as usize).await;

    // TODO remove
    let identity_stakes = IdentityStakesData {
        peer_type: ConnectionPeerType::Staked,
        stakes: 30,
        min_stakes: 0,
        max_stakes: 40,
        total_stakes: 100,
    };


    let (sender, _) = tokio::sync::broadcast::channel(MAXIMUM_TRANSACTIONS_IN_QUEUE);
    let broadcast_sender = Arc::new(sender);

    let mut connections_to_keep: HashMap<Pubkey, SocketAddr> = HashMap::new();


    loop {
        if exit_signal.load(Ordering::Relaxed) {
            bail!("exit signal received");
        }

        let forward_packet = Arc::new(
            transaction_channel
                .recv()
                .await
                .expect("channel closed unexpectedly"),
        );
        let tpu_address = forward_packet.tpu_address;
        let tpu_identity = forward_packet.tpu_identity;

        connections_to_keep.insert(tpu_identity, tpu_address);

        // TODO optimize
        tpu_connection_manager
            .update_connections(
                broadcast_sender.clone(),
                &connections_to_keep,
                identity_stakes,
                DataCache::new_for_tests(),
                QUIC_CONNECTION_PARAMS, // TODO improve
            )
            .await;


        for raw_tx in &forward_packet.transactions {
            let tx = bincode::deserialize::<VersionedTransaction>(&raw_tx).unwrap();

            let tsi = SentTransactionInfo {
                signature: tx.get_signature().to_string(),
                slot: 4242,
                transaction: raw_tx.clone(),
                last_valid_block_height: 999,
            };
            broadcast_sender.send(tsi)?;
        }

    } // all txs in packet

}
