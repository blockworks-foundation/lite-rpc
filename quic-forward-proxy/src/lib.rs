// lib definition is only required for 'quic-forward-proxy-integration-test' to work

use const_env::from_env;

mod cli;
mod inbound;
mod outbound;
pub mod proxy;
pub mod proxy_request_format;
mod quic_util;
pub mod tls_config_provider_client;
pub mod tls_config_provider_server;
pub mod tls_self_signed_pair_generator;
mod util;
pub mod validator_identity;


