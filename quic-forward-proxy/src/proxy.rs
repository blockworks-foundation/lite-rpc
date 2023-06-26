use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use anyhow::{anyhow, bail};
use log::{error, info, warn};
use quinn::{Connecting, Endpoint, SendStream, ServerConfig};
use rcgen::generate_simple_self_signed;
use rustls::{Certificate, PrivateKey};
use rustls::server::ResolvesServerCert;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::transaction::VersionedTransaction;
use tokio::net::ToSocketAddrs;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;
use solana_lite_rpc_core::proxy_request_format::TpuForwardingRequest;
use solana_lite_rpc_services::tpu_utils::tpu_connection_manager::ActiveConnection;
use crate::tls_config_provicer::{ProxyTlsConfigProvider, SelfSignedTlsConfigProvider};


pub struct QuicForwardProxy {
    endpoint: Endpoint,
}

impl QuicForwardProxy {
    pub async fn new(
        proxy_listener_addr: SocketAddr,
        tls_config: &SelfSignedTlsConfigProvider) -> anyhow::Result<Self> {
        let server_tls_config = tls_config.get_server_tls_crypto_config();

        let mut quinn_server_config = ServerConfig::with_crypto(Arc::new(server_tls_config));

        let endpoint = Endpoint::server(quinn_server_config, proxy_listener_addr).unwrap();
        info!("listening on {}", endpoint.local_addr()?);



        Ok(Self {endpoint})

    }

    pub async fn start_services(
        mut self,
    ) -> anyhow::Result<()> {
        let endpoint = self.endpoint.clone();
        let quic_proxy: AnyhowJoinHandle = tokio::spawn(async move {
            info!("TPU Quic Proxy server start on {}", endpoint.local_addr()?);

            let identity_keypair = Keypair::new(); // TODO

            while let Some(conn) = endpoint.accept().await {
                info!("connection incoming");
                let fut = handle_connection2(conn);
                tokio::spawn(async move {
                    if let Err(e) = fut.await {
                        error!("connection failed: {reason}", reason = e.to_string())
                    }
                });
            }

            // while let Some(conn) = endpoint.accept().await {
            //     info!("connection incoming");
            //     // let fut = handle_connection(conn);
            //     tokio::spawn(async move {
            //         info!("start thread");
            //         handle_connection2(conn).await.unwrap();
            //         // if let Err(e) = fut.await {
            //         //     error!("connection failed: {reason}", reason = e.to_string())
            //         // }
            //     });
            // }

            bail!("TPU Quic Proxy server stopped");
        });

        tokio::select! {
            res = quic_proxy => {
                bail!("TPU Quic Proxy server exited unexpectedly {res:?}");
            },
        }
    }

}


// meins
async fn handle_connection2(connecting: Connecting) -> anyhow::Result<()> {
    let connection = connecting.await?;
    info!("connection established, remote {connection}", connection = connection.remote_address());

    info!("established");
    async {
        loop {
            let stream = connection.accept_uni().await;
            let mut recv = match stream {
                Err(quinn::ConnectionError::ApplicationClosed { .. }) => {
                    info!("connection closed");
                    return Ok(());
                }
                Err(e) => {
                    warn!("connection failed: {}", e);
                    return Err(anyhow::Error::msg("connection failed"));
                }
                Ok(s) => s,
            };
            tokio::spawn(async move {
                let raw_request = recv.read_to_end(100000).await
                    .unwrap();
                // let str = std::str::from_utf8(&result).unwrap();
                info!("read proxy_request {:02X?}", raw_request);

                let proxy_request = match bincode::deserialize::<TpuForwardingRequest>(&raw_request) {
                    Ok(raw_request) => raw_request,
                    Err(err) => {
                        warn!("failed to deserialize proxy request: {:?}", err);
                        // bail!(err.to_string());
                        return;
                    }
                };

                info!("transaction details: {} sigs", proxy_request.get_transactions().len());

                // ActiveConnection::new(e)new(tx).await;

                // send_data(send).await;
                // Ok(())
            });
            // info!("stream okey {:?}", stream);
            // let fut = handle_request2(stream).await;
            // tokio::spawn(
            //     async move {
            //         if let Err(e) = fut.await {
            //             error!("failed: {reason}", reason = e.to_string());
            //         }
            //     }
            // );
        } // -- loop
    }
        .await?;
    Ok(())
}


async fn send_data(mut send: SendStream) -> anyhow::Result<()> {
    send.write_all(b"HELLO STRANGER\r\n").await?;
    send.finish().await?;
    Ok(())
}

async fn handle_request2(
    (mut send, recv): (quinn::SendStream, quinn::RecvStream),
) -> anyhow::Result<()> {
    info!("handle incoming request...");

    send.write_all(b"HELLO STRANGER\r\n").await?;
    send.finish().await?;

    Ok(())
}
