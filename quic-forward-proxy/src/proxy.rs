use std::net::SocketAddr;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use anyhow::{anyhow, bail, Context};

use log::{debug, error, info, trace};
use crate::inbound::proxy_listener;
use crate::outbound::tx_forward::tx_forwarder;
use crate::tls_config_provider::{ProxyTlsConfigProvider, SelfSignedTlsConfigProvider};
use crate::util::AnyhowJoinHandle;
use crate::validator_identity::ValidatorIdentity;


pub struct QuicForwardProxy {
    // endpoint: Endpoint,
    validator_identity: ValidatorIdentity,
    tls_config: Arc<SelfSignedTlsConfigProvider>,
    pub proxy_listener_addr: SocketAddr,
}

impl QuicForwardProxy {
    pub async fn new(
        proxy_listener_addr: SocketAddr,
        tls_config: Arc<SelfSignedTlsConfigProvider>,
        validator_identity: ValidatorIdentity) -> anyhow::Result<Self> {

        info!("Quic proxy uses validator identity {}", validator_identity);

        Ok(Self { proxy_listener_addr, validator_identity, tls_config })

    }

    pub async fn start_services(
        self,
    ) -> anyhow::Result<()> {
        let exit_signal = Arc::new(AtomicBool::new(false));

        let (forwarder_channel, forward_receiver) = tokio::sync::mpsc::channel(100_000);

        let proxy_listener = proxy_listener::ProxyListener::new(
            self.proxy_listener_addr,
            self.tls_config);

        let exit_signal_clone = exit_signal.clone();
        let quic_proxy = tokio::spawn(async move {

            proxy_listener.listen(exit_signal_clone.clone(), forwarder_channel).await
                .expect("proxy listen service");
        });

        let validator_identity = self.validator_identity.clone();
        let exit_signal_clone = exit_signal.clone();
        let forwarder: AnyhowJoinHandle = tokio::spawn(tx_forwarder(validator_identity,
                                                                    forward_receiver, exit_signal_clone));

        tokio::select! {
            res = quic_proxy => {
                bail!("TPU Quic Proxy server exited unexpectedly {res:?}");
            },
            res = forwarder => {
                bail!("TPU Quic Tx forwarder exited unexpectedly {res:?}");
            },
        }
    }

}




