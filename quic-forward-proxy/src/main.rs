use crate::cli::Args;
use crate::proxy::QuicForwardProxy;
use crate::tls_self_signed_pair_generator::SelfSignedTlsConfigProvider;
use anyhow::bail;
use clap::Parser;
use dotenv::dotenv;
use log::info;
use std::sync::Arc;
use solana_lite_rpc_core::keypair_loader::load_identity_keypair;

use crate::validator_identity::ValidatorIdentity;

pub mod cli;
mod inbound;
mod outbound;
pub mod proxy;
pub mod proxy_request_format;
pub mod quic_util;
mod quinn_auto_reconnect;
mod shared;
pub mod tls_config_provider_client;
pub mod tls_config_provider_server;
pub mod tls_self_signed_pair_generator;
mod util;
mod validator_identity;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let Args {
        identity_keypair,
        proxy_listen_addr,
    } = Args::parse();
    dotenv().ok();

    let proxy_listener_addr = proxy_listen_addr.parse().unwrap();
    let validator_identity = ValidatorIdentity::new(load_identity_keypair(&identity_keypair).await);

    let tls_config = Arc::new(SelfSignedTlsConfigProvider::new_singleton_self_signed_localhost());
    let main_services = QuicForwardProxy::new(proxy_listener_addr, tls_config, validator_identity)
        .await?
        .start_services();

    let ctrl_c_signal = tokio::signal::ctrl_c();

    tokio::select! {
        res = main_services => {
            bail!("Services quit unexpectedly {res:?}");
        },
        // res = test_client => {
        //     bail!("Test Client quit unexpectedly {res:?}");
        // },
        _ = ctrl_c_signal => {
            info!("Received ctrl+c signal");

            Ok(())
        }
    }
}
