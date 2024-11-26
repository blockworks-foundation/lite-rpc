use crate::quic_util::{SkipServerVerification, ALPN_TPU_FORWARDPROXY_PROTOCOL_ID};
use crate::tls_config_provider_client::TpuClientTlsConfigProvider;
use crate::tls_config_provider_server::ProxyTlsConfigProvider;
use rcgen::generate_simple_self_signed;
use rustls::{Certificate, ClientConfig, PrivateKey, ServerConfig};

impl ProxyTlsConfigProvider for SelfSignedTlsConfigProvider {
    fn get_server_tls_crypto_config(&self) -> ServerConfig {
        self.server_crypto.clone()
    }
}

impl TpuClientTlsConfigProvider for SelfSignedTlsConfigProvider {
    fn get_client_tls_crypto_config(&self) -> ClientConfig {
        self.client_crypto.clone()
    }
}

pub struct SelfSignedTlsConfigProvider {
    client_crypto: ClientConfig,
    server_crypto: ServerConfig,
}

impl SelfSignedTlsConfigProvider {
    pub fn new_singleton_self_signed_localhost() -> Self {
        // note: this check could be relaxed when you know what you are doing!
        let hostnames = vec!["localhost".to_string()];
        let (certificate, private_key) = Self::gen_tls_certificate_and_key(hostnames);
        let server_crypto = Self::build_server_crypto(certificate, private_key);
        Self {
            client_crypto: Self::build_client_crypto_insecure(),
            server_crypto,
        }
    }

    fn gen_tls_certificate_and_key(hostnames: Vec<String>) -> (Certificate, PrivateKey) {
        let cert = generate_simple_self_signed(hostnames).unwrap();
        let key = cert.key_pair.serialize_der();
        (Certificate(cert.key_pair.serialize_der()), PrivateKey(key))
    }

    fn build_client_crypto_insecure() -> ClientConfig {
        let mut client_crypto = rustls::ClientConfig::builder()
            .with_safe_defaults()
            // .with_root_certificates(roots)
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth();
        client_crypto.enable_early_data = true;
        client_crypto.alpn_protocols = vec![ALPN_TPU_FORWARDPROXY_PROTOCOL_ID.to_vec()];
        client_crypto
    }

    fn build_server_crypto(server_cert: Certificate, server_key: PrivateKey) -> ServerConfig {
        // let (server_cert, server_key) = gen_tls_certificate_and_key();

        let mut server_crypto = rustls::ServerConfig::builder()
            // FIXME we want client auth
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(vec![server_cert], server_key)
            .unwrap();
        server_crypto.alpn_protocols = vec![ALPN_TPU_FORWARDPROXY_PROTOCOL_ID.to_vec()];
        server_crypto
    }

    pub fn get_client_tls_crypto_config(&self) -> &ClientConfig {
        &self.client_crypto
    }
}
