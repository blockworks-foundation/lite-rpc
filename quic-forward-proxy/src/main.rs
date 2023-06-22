use std::net::{IpAddr, SocketAddr};
use anyhow::bail;
use log::info;
use crate::proxy::QuicForwardProxy;
use crate::test_client::quic_test_client::QuicTestClient;
use crate::tls_config_provicer::SelfSignedTlsConfigProvider;

mod proxy;
mod test_client;
mod quic_util;
mod tls_config_provicer;


#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let proxy_listener_addr = "127.0.0.1:11111".parse().unwrap();
    let tls_configuration = SelfSignedTlsConfigProvider::new_singleton_self_signed_localhost();


    let main_services = QuicForwardProxy::new(proxy_listener_addr, &tls_configuration)
        .await?
        .start_services();

    let proxy_addr = "127.0.0.1:11111".parse().unwrap();
    let test_client = QuicTestClient::new_with_endpoint(
        proxy_addr, &tls_configuration)
        .await?
        .start_services();


    let ctrl_c_signal = tokio::signal::ctrl_c();

    tokio::select! {
        res = main_services => {
            bail!("Services quit unexpectedly {res:?}");
        },
        res = test_client => {
            bail!("Test Client quit unexpectedly {res:?}");
        },
        _ = ctrl_c_signal => {
            info!("Received ctrl+c signal");

            Ok(())
        }
    }

}
