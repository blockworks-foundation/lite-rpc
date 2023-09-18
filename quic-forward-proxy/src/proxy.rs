use std::net::{SocketAddr, SocketAddrV4};

use anyhow::bail;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::inbound::proxy_listener;
use crate::outbound::tx_forward::tx_forwarder;
use crate::tls_self_signed_pair_generator::SelfSignedTlsConfigProvider;
use crate::util::AnyhowJoinHandle;
use crate::validator_identity::ValidatorIdentity;
use log::info;

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
        validator_identity: ValidatorIdentity,
    ) -> anyhow::Result<Self> {
        info!("Quic proxy uses validator identity {}", validator_identity);

        Ok(Self {
            proxy_listener_addr,
            validator_identity,
            tls_config,
        })
    }

    pub async fn start_services(self) -> anyhow::Result<()> {
        let exit_signal = Arc::new(AtomicBool::new(false));

        let (forwarder_channel, forward_receiver) = tokio::sync::mpsc::channel(1000);

        let proxy_listener =
            proxy_listener::ProxyListener::new(self.proxy_listener_addr, self.tls_config);

        let quic_proxy = tokio::spawn(async move {
            proxy_listener
                .listen(&forwarder_channel)
                .await
                .expect("proxy listen service");
        });

        let validator_identity = self.validator_identity.clone();
        let exit_signal_clone = exit_signal.clone();
        let forwarder: AnyhowJoinHandle = tokio::spawn(tx_forwarder(
            validator_identity,
            forward_receiver,
            exit_signal_clone,
        ));

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
