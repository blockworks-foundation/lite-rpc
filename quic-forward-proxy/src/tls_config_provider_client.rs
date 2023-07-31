use std::sync::atomic::{AtomicU32, Ordering};
use rcgen::generate_simple_self_signed;
use rustls::{Certificate, ClientConfig, PrivateKey, ServerConfig};
use crate::tpu_quic_connection_utils::SkipServerVerification;
use crate::quic_util::ALPN_TPU_FORWARDPROXY_PROTOCOL_ID;

// TODO integrate with tpu_service + quic_connection_utils

pub trait TpuCLientTlsConfigProvider {

    fn get_client_tls_crypto_config(&self) -> ClientConfig;

}
