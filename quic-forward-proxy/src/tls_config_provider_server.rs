use rustls::ServerConfig;

// TODO integrate with tpu_service + quic_connection_utils

pub trait ProxyTlsConfigProvider {
    fn get_server_tls_crypto_config(&self) -> ServerConfig;
}
