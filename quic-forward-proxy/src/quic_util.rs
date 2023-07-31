use std::sync::Arc;
use quinn::Connection;

pub const ALPN_TPU_FORWARDPROXY_PROTOCOL_ID: &[u8] = b"solana-tpu-forward-proxy";


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
