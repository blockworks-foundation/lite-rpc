use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::thread::sleep;
use std::time::Duration;
use tracing::{debug_span, instrument, Instrument, span};
use anyhow::{anyhow, bail, Context};
use dashmap::DashMap;
use itertools::{any, Itertools};
use log::{debug, error, info, trace, warn};
use quinn::{Connecting, Connection, Endpoint, SendStream, ServerConfig, VarInt};
use rcgen::generate_simple_self_signed;
use rustls::{Certificate, PrivateKey};
use rustls::server::ResolvesServerCert;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::VersionedTransaction;
use tokio::net::ToSocketAddrs;
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use tokio::sync::RwLock;
use crate::proxy_request_format::TpuForwardingRequest;
use crate::quic_connection_utils::QuicConnectionUtils;
use crate::tpu_quic_client::{TpuQuicClient};
use crate::tls_config_provicer::{ProxyTlsConfigProvider, SelfSignedTlsConfigProvider};
use crate::util::AnyhowJoinHandle;

pub struct QuicForwardProxy {
    endpoint: Endpoint,
    validator_identity: Arc<Keypair>,
    tpu_quic_client: TpuQuicClient,
}

impl QuicForwardProxy {
    pub async fn new(
        proxy_listener_addr: SocketAddr,
        tls_config: &SelfSignedTlsConfigProvider,
        validator_identity: Arc<Keypair>) -> anyhow::Result<Self> {
        let server_tls_config = tls_config.get_server_tls_crypto_config();

        let mut quinn_server_config = ServerConfig::with_crypto(Arc::new(server_tls_config));

        let endpoint = Endpoint::server(quinn_server_config, proxy_listener_addr).unwrap();
        info!("tpu forward proxy listening on {}", endpoint.local_addr()?);
        info!("staking from validator identity {}", validator_identity.pubkey());

        let tpu_quic_client =
            TpuQuicClient::new_with_validator_identity(validator_identity.as_ref()).await;

        Ok(Self { endpoint, validator_identity, tpu_quic_client })

    }

    pub async fn start_services(
        mut self,
    ) -> anyhow::Result<()> {
        let exit_signal = Arc::new(AtomicBool::new(false));

        let endpoint = self.endpoint.clone();
        let quic_proxy: AnyhowJoinHandle = tokio::spawn(async move {
            info!("TPU Quic Proxy server start on {}", endpoint.local_addr()?);


            while let Some(connecting) = endpoint.accept().await {

                let exit_signal = exit_signal.clone();
                let validator_identity_copy = self.validator_identity.clone();
                let tpu_quic_client = self.tpu_quic_client.clone();
                tokio::spawn(async move {

                    let connection = connecting.await.context("accept connection").unwrap();
                    match accept_client_connection(connection, tpu_quic_client, exit_signal, validator_identity_copy)
                        .await {
                        Ok(()) => {}
                        Err(err) => {
                            error!("setup connection failed: {reason}", reason = err);
                        }
                    }

                });
            }

            bail!("TPU Quic Proxy server stopped");
        });

        tokio::select! {
            res = quic_proxy => {
                bail!("TPU Quic Proxy server exited unexpectedly {res:?}");
            },
        }
    }

}


// TODO use interface abstraction for connection_per_tpunode
#[tracing::instrument(skip_all, level = "debug")]
async fn accept_client_connection(client_connection: Connection, tpu_quic_client: TpuQuicClient,
                                  exit_signal: Arc<AtomicBool>, validator_identity: Arc<Keypair>) -> anyhow::Result<()> {
    debug!("inbound connection established, client {}", client_connection.remote_address());

    // let active_tpu_connection =
    //     TpuQuicClient::new_with_validator_identity(validator_identity.as_ref()).await;

    loop {
        let maybe_stream = client_connection.accept_uni().await;
        let mut recv_stream = match maybe_stream {
            Err(quinn::ConnectionError::ApplicationClosed(reason)) => {
                debug!("connection closed by peer - reason: {:?}", reason);
                if reason.error_code != VarInt::from_u32(0) {
                    return Err(anyhow!("connection closed by peer with unexpected reason: {:?}", reason));
                }
                debug!("connection gracefully closed by peer");
                return Ok(());
            },
            Err(e) => {
                error!("failed to accept stream: {}", e);
                return Err(anyhow::Error::msg("error accepting stream"));
            }
            Ok(s) => s,
        };
        let exit_signal_copy = exit_signal.clone();
        let validator_identity_copy = validator_identity.clone();
        let tpu_quic_client_copy = tpu_quic_client.clone();

        tokio::spawn(async move {

            let raw_request = recv_stream.read_to_end(10_000_000).await // TODO extract to const
                .unwrap();
            debug!("read proxy_request {} bytes", raw_request.len());

            let proxy_request = TpuForwardingRequest::deserialize_from_raw_request(&raw_request);

            debug!("proxy request details: {}", proxy_request);
            let tpu_identity = proxy_request.get_identity_tpunode();
            let tpu_address = proxy_request.get_tpu_socket_addr();
            let txs = proxy_request.get_transactions();

            // TODO join get_or_create_connection future and read_to_end
            let tpu_connection = tpu_quic_client_copy.get_or_create_connection(tpu_address).await;

            info!("send transaction batch of size {} to address {}", txs.len(), tpu_address);
            tpu_quic_client_copy.send_txs_to_tpu(tpu_connection, &txs, exit_signal_copy).await;

            // active_tpu_connection_copy.send_txs_to_tpu(exit_signal_copy, validator_identity_copy, tpu_identity, tpu_address, &txs).await;

        })
            .await.unwrap();

    } // -- loop
}
