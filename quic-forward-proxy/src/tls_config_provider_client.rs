use rustls::ClientConfig;

// TODO integrate with tpu_service + quic_connection_utils

pub trait TpuCLientTlsConfigProvider {
    fn get_client_tls_crypto_config(&self) -> ClientConfig;
}
