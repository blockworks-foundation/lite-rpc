use crate::util::timeout_fallback;
use anyhow::{bail, Context};
use log::{info, warn};
use quinn::{Connection, ConnectionError, Endpoint};
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::RwLock;
use tracing::debug;

enum ConnectionState {
    NotConnected,
    Connection(Connection),
    PermanentError,
}

pub struct AutoReconnect {
    // endoint should be configures with keep-alive and idle timeout
    endpoint: Endpoint,
    current: RwLock<ConnectionState>,
    pub target_address: SocketAddr,
    reconnect_count: AtomicU32,
}

impl AutoReconnect {
    pub fn new(endpoint: Endpoint, target_address: SocketAddr) -> Self {
        Self {
            endpoint,
            current: RwLock::new(ConnectionState::NotConnected),
            target_address,
            reconnect_count: AtomicU32::new(0),
        }
    }

    pub async fn send_uni(&self, payload: Vec<u8>) -> anyhow::Result<()> {
        let mut send_stream = timeout_fallback(self.refresh_and_get().await?.open_uni())
            .await
            .context("open uni stream for sending")??;
        send_stream.write_all(payload.as_slice()).await?;
        send_stream.finish().await?;
        Ok(())
    }

    pub async fn refresh_and_get(&self) -> anyhow::Result<Connection> {
        self.refresh().await;

        let lock = self.current.read().await;
        match &*lock {
            ConnectionState::NotConnected => bail!("not connected"),
            ConnectionState::Connection(conn) => Ok(conn.clone()),
            ConnectionState::PermanentError => bail!("permanent error"),
        }
    }

    pub async fn refresh(&self) {
        {
            // first check for existing connection using a cheap read-lock
            let lock = self.current.read().await;
            if let ConnectionState::Connection(conn) = &*lock {
                if conn.close_reason().is_none() {
                    debug!("Reuse connection {}", conn.stable_id());
                    return;
                }
            }
        }
        let mut lock = self.current.write().await;
        match &*lock {
            ConnectionState::Connection(current) => {
                if current.close_reason().is_some() {
                    let old_stable_id = current.stable_id();
                    warn!(
                        "Connection {} is closed for reason: {:?}",
                        old_stable_id,
                        current.close_reason()
                    );

                    match self.create_connection().await {
                        Some(new_connection) => {
                            *lock = ConnectionState::Connection(new_connection.clone());
                            let reconnect_count =
                                self.reconnect_count.fetch_add(1, Ordering::SeqCst);

                            if reconnect_count < 10 {
                                info!(
                                    "Replace closed connection {} with {} (retry {})",
                                    old_stable_id,
                                    new_connection.stable_id(),
                                    reconnect_count
                                );
                            } else {
                                *lock = ConnectionState::PermanentError;
                                warn!(
                                    "Too many reconnect attempts {} with {} (retry {})",
                                    old_stable_id,
                                    new_connection.stable_id(),
                                    reconnect_count
                                );
                            }
                        }
                        None => {
                            warn!(
                                "Reconnect to {} failed for connection {}",
                                self.target_address, old_stable_id
                            );
                            *lock = ConnectionState::PermanentError;
                        }
                    };
                } else {
                    debug!("Reuse connection {} with write-lock", current.stable_id());
                }
            }
            ConnectionState::NotConnected => {
                match self.create_connection().await {
                    Some(new_connection) => {
                        *lock = ConnectionState::Connection(new_connection.clone());
                        self.reconnect_count.fetch_add(1, Ordering::SeqCst);

                        info!(
                            "Create initial connection {} to {}",
                            new_connection.stable_id(),
                            self.target_address
                        );
                    }
                    None => {
                        warn!(
                            "Initial connection to {} failed permanently",
                            self.target_address
                        );
                        *lock = ConnectionState::PermanentError;
                    }
                };
            }
            ConnectionState::PermanentError => {
                // no nothing
                debug!("Not using connection with permanent error");
            }
        }
    }

    async fn create_connection(&self) -> Option<Connection> {
        let connection = self
            .endpoint
            .connect(self.target_address, "localhost")
            .expect("handshake");

        match connection.await {
            Ok(conn) => Some(conn),
            Err(ConnectionError::TimedOut) => None,
            // maybe we should also treat TransportError explicitly
            Err(unexpected_error) => {
                panic!(
                    "Connection to {} failed with unexpected error: {}",
                    self.target_address, unexpected_error
                );
            }
        }
    }

    //  stable_id 140266619216912, rtt=2.156683ms,
    // stats FrameStats { ACK: 3, CONNECTION_CLOSE: 0, CRYPTO: 3,
    // DATA_BLOCKED: 0, DATAGRAM: 0, HANDSHAKE_DONE: 1, MAX_DATA: 0,
    // MAX_STREAM_DATA: 1, MAX_STREAMS_BIDI: 0, MAX_STREAMS_UNI: 0, NEW_CONNECTION_ID: 4,
    // NEW_TOKEN: 0, PATH_CHALLENGE: 0, PATH_RESPONSE: 0, PING: 0, RESET_STREAM: 0,
    // RETIRE_CONNECTION_ID: 1, STREAM_DATA_BLOCKED: 0, STREAMS_BLOCKED_BIDI: 0,
    // STREAMS_BLOCKED_UNI: 0, STOP_SENDING: 0, STREAM: 0 }
    pub async fn connection_stats(&self) -> String {
        let lock = self.current.read().await;
        match &*lock {
            ConnectionState::Connection(conn) => format!(
                "stable_id {} stats {:?}, rtt={:?}",
                conn.stable_id(),
                conn.stats().frame_rx,
                conn.stats().path.rtt
            ),
            ConnectionState::NotConnected => "n/c".to_string(),
            ConnectionState::PermanentError => "n/a (permanent)".to_string(),
        }
    }
}

impl fmt::Display for AutoReconnect {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Connection to {}", self.target_address,)
    }
}
