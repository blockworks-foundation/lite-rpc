use crate::validator_identity::ValidatorIdentity;
use anyhow::{bail};
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::quic::QUIC_MIN_STAKED_CONCURRENT_STREAMS;
use tokio::sync::mpsc::Receiver;
use solana_lite_rpc_core::quic_connection_utils::QuicConnectionParameters;
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
    fanout_slots: u64,
) -> anyhow::Result<()> {

    let (certificate, key) = new_self_signed_tls_certificate(
        &validator_identity.get_keypair_for_tls(),
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
    )
        .expect("Failed to initialize QUIC connection certificates");

    let tpu_connection_manager =
        TpuConnectionManager::new(certificate, key, fanout_slots as usize).await;

    let max_uni_stream_connections = QUIC_MIN_STAKED_CONCURRENT_STREAMS;

    loop {
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
            requested_connections.insert(tpu_node.identity_tpunode, tpu_node.tpu_socket_addr);
        }

        tpu_connection_manager
            .update_connections(
                &requested_connections,
                max_uni_stream_connections,
                QUIC_CONNECTION_PARAMS, // TODO improve
            )
            .await;

        for raw_tx in forward_packet.get_transaction_bytes() {
            tpu_connection_manager.send_transaction(raw_tx);
        }

    } // all txs in packet

}
