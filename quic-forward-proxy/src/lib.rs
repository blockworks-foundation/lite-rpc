// lib definition is only required for 'quic-forward-proxy-integration-test' to work

mod cli;
mod inbound;
pub mod outbound;
pub mod proxy;
pub mod proxy_request_format;
mod quic_util;
mod quinn_auto_reconnect;
pub mod shared;
pub mod tls_config_provider_client;
pub mod tls_config_provider_server;
pub mod tls_self_signed_pair_generator;
mod util;
pub mod validator_identity;
