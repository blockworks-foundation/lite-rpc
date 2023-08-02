use rustls::ClientConfig;

pub trait TpuClientTlsConfigProvider {
    fn get_client_tls_crypto_config(&self) -> ClientConfig;
}
