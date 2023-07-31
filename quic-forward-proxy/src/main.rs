use std::sync::Arc;
use anyhow::bail;
use clap::Parser;
use dotenv::dotenv;
use log::info;
use crate::cli::{Args, get_identity_keypair};
use crate::proxy::QuicForwardProxy;

pub use tls_config_provider::SelfSignedTlsConfigProvider;
use crate::validator_identity::ValidatorIdentity;


pub mod quic_util;
pub mod tls_config_provider;
pub mod proxy;
pub mod proxy_request_format;
pub mod cli;
pub mod test_client;
mod util;
mod tx_store;
mod tpu_quic_connection_utils;
mod quinn_auto_reconnect;
mod outbound;
mod inbound;
mod shared;
mod validator_identity;


#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let Args {
        identity_keypair,
        proxy_listen_addr: proxy_rpc_addr,
    } = Args::parse();

    dotenv().ok();

    // TODO build args struct dedicated to proxy
    let proxy_listener_addr = proxy_rpc_addr.parse().unwrap();
    let _tls_configuration = SelfSignedTlsConfigProvider::new_singleton_self_signed_localhost();
    let validator_identity =
        ValidatorIdentity::new(get_identity_keypair(&identity_keypair).await);

    let tls_config = Arc::new(SelfSignedTlsConfigProvider::new_singleton_self_signed_localhost());
    let main_services = QuicForwardProxy::new(proxy_listener_addr, tls_config, validator_identity)
        .await?
        .start_services();

    // let proxy_addr = "127.0.0.1:11111".parse().unwrap();
    // let test_client = QuicTestClient::new_with_endpoint(
    //     proxy_addr, &tls_configuration)
    //     .await?
    //     .start_services();


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
