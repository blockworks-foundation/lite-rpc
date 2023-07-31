use log::{debug, error, trace, warn};
use quinn::{ClientConfig, Connection, ConnectionError, Endpoint, EndpointConfig, IdleTimeout, SendStream, TokioRuntime, TransportConfig, VarInt};
use solana_sdk::pubkey::Pubkey;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::future::join_all;
use itertools::Itertools;
use solana_sdk::quic::QUIC_MAX_TIMEOUT_MS;
use tokio::{time::timeout};



const ALPN_TPU_PROTOCOL_ID: &[u8] = b"solana-tpu";

pub struct QuicConnectionUtils {}

pub enum QuicConnectionError {
    TimeOut,
    ConnectionError { retry: bool },
}

// TODO check whot we need from this
#[derive(Clone, Copy)]
pub struct QuicConnectionParameters {
    // pub connection_timeout: Duration,
    pub unistream_timeout: Duration,
    pub write_timeout: Duration,
    pub finalize_timeout: Duration,
    pub connection_retry_count: usize,
    // pub max_number_of_connections: usize,
    // pub number_of_transactions_per_unistream: usize,
}

impl QuicConnectionUtils {
    // TODO move to a more specific place
    pub fn create_tpu_client_endpoint(certificate: rustls::Certificate, key: rustls::PrivateKey) -> Endpoint {
        let mut endpoint = {
            let client_socket =
                solana_net_utils::bind_in_range(IpAddr::V4(Ipv4Addr::UNSPECIFIED), (8000, 10000))
                    .expect("create_endpoint bind_in_range")
                    .1;
            let config = EndpointConfig::default();
            quinn::Endpoint::new(config, None, client_socket, TokioRuntime)
                .expect("create_endpoint quinn::Endpoint::new")
        };

        let mut crypto = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_single_cert(vec![certificate], key)
            .expect("Failed to set QUIC client certificates");

        crypto.enable_early_data = true;

        crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

        let mut config = ClientConfig::new(Arc::new(crypto));

        // note: this should be aligned with solana quic server's endpoint config
        let mut transport_config = TransportConfig::default();
        // no remotely-initiated streams required
        transport_config.max_concurrent_uni_streams(VarInt::from_u32(0));
        transport_config.max_concurrent_bidi_streams(VarInt::from_u32(0));
        let timeout = IdleTimeout::try_from(Duration::from_millis(QUIC_MAX_TIMEOUT_MS as u64)).unwrap();
        transport_config.max_idle_timeout(Some(timeout));
        transport_config.keep_alive_interval(None);
        config.transport_config(Arc::new(transport_config));

        endpoint.set_default_client_config(config);

        endpoint
    }

}

pub struct SkipServerVerification;

impl SkipServerVerification {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

//  stable_id 140266619216912, rtt=2.156683ms,
// stats FrameStats { ACK: 3, CONNECTION_CLOSE: 0, CRYPTO: 3,
// DATA_BLOCKED: 0, DATAGRAM: 0, HANDSHAKE_DONE: 1, MAX_DATA: 0,
// MAX_STREAM_DATA: 1, MAX_STREAMS_BIDI: 0, MAX_STREAMS_UNI: 0, NEW_CONNECTION_ID: 4,
// NEW_TOKEN: 0, PATH_CHALLENGE: 0, PATH_RESPONSE: 0, PING: 0, RESET_STREAM: 0,
// RETIRE_CONNECTION_ID: 1, STREAM_DATA_BLOCKED: 0, STREAMS_BLOCKED_BIDI: 0,
// STREAMS_BLOCKED_UNI: 0, STOP_SENDING: 0, STREAM: 0 }
pub fn connection_stats(connection: &Connection) -> String {
    format!("stable_id {} stats {:?}, rtt={:?}",
            connection.stable_id(), connection.stats().frame_rx, connection.stats().path.rtt)
}