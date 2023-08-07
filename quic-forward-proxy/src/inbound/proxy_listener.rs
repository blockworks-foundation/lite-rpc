use crate::proxy_request_format::TpuForwardingRequest;
use crate::quic_util::connection_stats;
use crate::shared::ForwardPacket;
use crate::tls_config_provider_server::ProxyTlsConfigProvider;
use crate::tls_self_signed_pair_generator::SelfSignedTlsConfigProvider;
use crate::util::FALLBACK_TIMEOUT;
use anyhow::{anyhow, bail, Context};
use log::{debug, error, info, trace};
use quinn::{Connection, Endpoint, ServerConfig, VarInt};
use solana_sdk::packet::PACKET_DATA_SIZE;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

// note: setting this to "1" did not make a difference!
// solana server sets this to 256
const MAX_CONCURRENT_UNI_STREAMS: u32 = 24;

pub struct ProxyListener {
    tls_config: Arc<SelfSignedTlsConfigProvider>,
    proxy_listener_addr: SocketAddr,
}

impl ProxyListener {
    pub fn new(
        proxy_listener_addr: SocketAddr,
        tls_config: Arc<SelfSignedTlsConfigProvider>,
    ) -> Self {
        Self {
            proxy_listener_addr,
            tls_config,
        }
    }

    pub async fn listen(
        &self,
        exit_signal: Arc<AtomicBool>,
        forwarder_channel: &Sender<ForwardPacket>,
    ) -> anyhow::Result<()> {
        info!(
            "TPU Quic Proxy server listening on {}",
            self.proxy_listener_addr
        );

        let endpoint =
            Self::new_proxy_listen_server_endpoint(&self.tls_config, self.proxy_listener_addr)
                .await;

        while let Some(connecting) = endpoint.accept().await {
            let exit_signal = exit_signal.clone();
            let forwarder_channel_copy = forwarder_channel.clone();
            tokio::spawn(async move {
                let connection = connecting.await.context("handshake").unwrap();
                match Self::accept_client_connection(
                    connection,
                    forwarder_channel_copy,
                    exit_signal,
                )
                .await
                {
                    Ok(()) => {
                        debug!("connection handles correctly");
                    }
                    Err(err) => {
                        error!(
                            "failed to accect connection from client: {reason} - skip",
                            reason = err
                        );
                    }
                }
            });
        }

        bail!("TPU Quic Proxy server stopped");
    }

    async fn new_proxy_listen_server_endpoint(
        tls_config: &SelfSignedTlsConfigProvider,
        proxy_listener_addr: SocketAddr,
    ) -> Endpoint {
        let server_tls_config = tls_config.get_server_tls_crypto_config();
        let mut quinn_server_config = ServerConfig::with_crypto(Arc::new(server_tls_config));

        // note: this config must be aligned with lite-rpc's client config
        let transport_config = Arc::get_mut(&mut quinn_server_config.transport).unwrap();
        transport_config.max_concurrent_uni_streams(VarInt::from_u32(MAX_CONCURRENT_UNI_STREAMS));
        // no bidi streams used
        transport_config.max_concurrent_bidi_streams(VarInt::from_u32(0));
        let timeout = Duration::from_secs(10).try_into().unwrap();
        transport_config.max_idle_timeout(Some(timeout));
        transport_config.keep_alive_interval(Some(Duration::from_millis(500)));
        transport_config.stream_receive_window((PACKET_DATA_SIZE as u32).into());
        transport_config
            .receive_window((PACKET_DATA_SIZE as u32 * MAX_CONCURRENT_UNI_STREAMS).into());

        Endpoint::server(quinn_server_config, proxy_listener_addr).unwrap()
    }

    #[tracing::instrument(skip_all, level = "debug")]
    async fn accept_client_connection(
        client_connection: Connection,
        forwarder_channel: Sender<ForwardPacket>,
        _exit_signal: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        debug!(
            "inbound connection established, client {}",
            client_connection.remote_address()
        );

        loop {
            let maybe_stream = client_connection.accept_uni().await;
            match maybe_stream {
                Err(quinn::ConnectionError::ApplicationClosed(reason)) => {
                    debug!("connection closed by client - reason: {:?}", reason);
                    if reason.error_code != VarInt::from_u32(0) {
                        return Err(anyhow!(
                            "connection closed by client with unexpected reason: {:?}",
                            reason
                        ));
                    }
                    debug!("connection gracefully closed by client");
                    return Ok(());
                }
                Err(e) => {
                    error!("failed to accept stream: {}", e);
                    bail!("error accepting stream");
                }
                Ok(recv_stream) => {
                    let forwarder_channel_copy = forwarder_channel.clone();
                    tokio::spawn(async move {
                        let raw_request = recv_stream.read_to_end(10_000_000).await.unwrap();

                        let proxy_request =
                            TpuForwardingRequest::deserialize_from_raw_request(&raw_request);

                        trace!("proxy request details: {}", proxy_request);
                        let _tpu_identity = proxy_request.get_identity_tpunode();
                        let tpu_address = proxy_request.get_tpu_socket_addr();
                        let txs = proxy_request.get_transactions();

                        debug!(
                            "enqueue transaction batch of size {} to address {}",
                            txs.len(),
                            tpu_address
                        );
                        if forwarder_channel_copy.capacity() < forwarder_channel_copy.max_capacity()
                        {
                            debug!(
                                "forward channel buffered: capacity {} of {}",
                                forwarder_channel_copy.capacity(),
                                forwarder_channel_copy.max_capacity()
                            );
                        }
                        forwarder_channel_copy
                            .send_timeout(
                                ForwardPacket {
                                    transactions: txs,
                                    tpu_address,
                                },
                                FALLBACK_TIMEOUT,
                            )
                            .await
                            .context("sending internal packet from proxy to forwarder")
                            .unwrap();
                    });

                    debug!(
                        "Inbound connection stats: {}",
                        connection_stats(&client_connection)
                    );
                }
            }; // -- result
        } // -- loop
    }
}
