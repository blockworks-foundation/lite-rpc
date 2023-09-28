use crate::validator_identity::ValidatorIdentity;
use anyhow::{bail};
use solana_streamer::nonblocking::quic::{compute_max_allowed_uni_streams, ConnectionPeerType};
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration};
use log::info;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc::Receiver;
use solana_lite_rpc_core::quic_connection_utils::QuicConnectionParameters;
use solana_lite_rpc_core::stores::data_cache::DataCache;
use solana_lite_rpc_core::structures::identity_stakes::IdentityStakesData;
use crate::outbound::tpu_connection_manager::{TpuConnectionManager};
use crate::proxy_request_format::TpuForwardingRequest;



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
    mut transaction_channel: Receiver<TpuForwardingRequest>,
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



    loop {
        info!("tick2");
        if exit_signal.load(Ordering::Relaxed) {
            bail!("exit signal received");
        }

        let forward_packet =
            transaction_channel
                .recv()
                .await
                .expect("channel closed unexpectedly");

        let mut requested_connections: HashMap<Pubkey, SocketAddr> = HashMap::new();
        for tpu_node in forward_packet.get_tpu_nodes() {
            // TODO optimize move into tpu_connection_manager and implement shutdown based on not used
            requested_connections.insert(tpu_node.identity_tpunode, tpu_node.tpu_socket_addr);
        }

        let max_uni_stream_connections = compute_max_allowed_uni_streams(
            identity_stakes.peer_type,
            identity_stakes.stakes,
            identity_stakes.total_stakes,
        );

        tpu_connection_manager
            .update_connections(
                &requested_connections,
                max_uni_stream_connections,
                DataCache::new_for_tests(),
                QUIC_CONNECTION_PARAMS, // TODO improve
            )
            .await;

        for raw_tx in forward_packet.get_transaction_bytes() {
            tpu_connection_manager.send_transaction(raw_tx);
        }

    } // all txs in packet

}
