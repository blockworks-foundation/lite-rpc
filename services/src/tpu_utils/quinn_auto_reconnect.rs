use anyhow::{bail, Context};
use log::{info, warn};
use quinn::{Connection, ConnectionError, Endpoint};
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::debug;

/// copy of quic-proxy AutoReconnect - used that for reference

const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_RETRY_ATTEMPTS: u32 = 10;

enum ConnectionState {
    NotConnected,
    Connection(Connection),
    PermanentError,
    FailedAttempt(u32),
}

pub struct AutoReconnect {
    // endoint should be configures with keep-alive and idle timeout
    endpoint: Endpoint,
    current: RwLock<ConnectionState>,
    pub target_address: SocketAddr,
}

impl AutoReconnect {
    pub fn new(endpoint: Endpoint, target_address: SocketAddr) -> Self {
        Self {
            endpoint,
            current: RwLock::new(ConnectionState::NotConnected),
            target_address,
        }
    }

    pub async fn is_permanent_dead(&self) -> bool {
        let lock = self.current.read().await;
        matches!(&*lock, ConnectionState::PermanentError)
    }

    pub async fn send_uni(&self, payload: &Vec<u8>) -> anyhow::Result<()> {
        let mut send_stream = timeout(SEND_TIMEOUT, self.refresh_and_get().await?.open_uni())
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
            ConnectionState::FailedAttempt(_) => bail!("failed connection attempt"),
        }
    }

    pub async fn refresh(&self) {
        {
            // first check for existing connection using a cheap read-lock
            let lock = self.current.read().await;
            if let ConnectionState::Connection(conn) = &*lock {
                if conn.close_reason().is_none() {
                    debug!(
                        "Reuse connection {} to {}",
                        conn.stable_id(),
                        self.target_address
                    );
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
                        "Connection {} to {} is closed for reason: {:?} - reconnecting",
                        old_stable_id,
                        self.target_address,
                        current.close_reason()
                    );

                    match self.create_connection().await {
                        Some(new_connection) => {
                            *lock = ConnectionState::Connection(new_connection.clone());
                            info!(
                                "Restored closed connection {} with {} to target {}",
                                old_stable_id,
                                new_connection.stable_id(),
                                self.target_address,
                            );
                        }
                        None => {
                            warn!(
                                "Reconnect to {} failed for connection {}",
                                self.target_address, old_stable_id
                            );
                            *lock = ConnectionState::FailedAttempt(1);
                        }
                    };
                } else {
                    debug!(
                        "Reuse connection {} to {} with write-lock",
                        current.stable_id(),
                        self.target_address
                    );
                }
            }
            ConnectionState::NotConnected => {
                match self.create_connection().await {
                    Some(new_connection) => {
                        *lock = ConnectionState::Connection(new_connection.clone());

                        info!(
                            "Create initial connection {} to {}",
                            new_connection.stable_id(),
                            self.target_address
                        );
                    }
                    None => {
                        warn!("Failed connect initially to target {}", self.target_address);
                        *lock = ConnectionState::FailedAttempt(1);
                    }
                };
            }
            ConnectionState::PermanentError => {
                // no nothing
                debug!(
                    "Not using connection to {} with permanent error",
                    self.target_address
                );
            }
            ConnectionState::FailedAttempt(attempts) => {
                match self.create_connection().await {
                    Some(new_connection) => {
                        *lock = ConnectionState::Connection(new_connection);
                    }
                    None => {
                        if *attempts < MAX_RETRY_ATTEMPTS {
                            warn!(
                                "Reconnect to {} failed (attempt {})",
                                self.target_address, attempts
                            );
                            *lock = ConnectionState::FailedAttempt(attempts + 1);
                        } else {
                            warn!(
                                "Reconnect to {} failed permanently (attempt {})",
                                self.target_address, attempts
                            );
                            *lock = ConnectionState::PermanentError;
                        }
                    }
                };
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
            ConnectionState::FailedAttempt(_) => "fail".to_string(),
        }
    }
}

impl fmt::Display for AutoReconnect {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Connection to {}", self.target_address,)
    }
}
