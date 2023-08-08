use anyhow::Context;
use log::warn;
use quinn::{Connection, Endpoint};
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::debug;

/// copy of quic-proxy AutoReconnect - used that for reference
pub struct AutoReconnect {
    // endoint should be configures with keep-alive and idle timeout
    endpoint: Endpoint,
    current: RwLock<Option<Connection>>,
    pub target_address: SocketAddr,
    reconnect_count: AtomicU32,
}

impl AutoReconnect {
    pub fn new(endpoint: Endpoint, target_address: SocketAddr) -> Self {
        Self {
            endpoint,
            current: RwLock::new(None),
            target_address,
            reconnect_count: AtomicU32::new(0),
        }
    }

    pub async fn send_uni(&self, payload: Vec<u8>) -> anyhow::Result<()> {
        // TOOD do smart error handling + reconnect
        let mut send_stream = timeout(Duration::from_secs(4), self.refresh().await.open_uni())
            .await
            .context("open uni stream for sending")??;
        send_stream.write_all(payload.as_slice()).await?;
        send_stream.finish().await?;
        Ok(())
    }

    pub async fn refresh(&self) -> Connection {
        {
            let lock = self.current.read().await;
            let maybe_conn = lock.as_ref();
            if maybe_conn
                .filter(|conn| conn.close_reason().is_none())
                .is_some()
            {
                let reuse = maybe_conn.unwrap();
                debug!("Reuse connection {}", reuse.stable_id());
                return reuse.clone();
            }
        }
        let mut lock = self.current.write().await;
        let maybe_conn = lock.as_ref();
        match maybe_conn {
            Some(current) => {
                if current.close_reason().is_some() {
                    let old_stable_id = current.stable_id();
                    warn!(
                        "Connection {} is closed for reason: {:?}",
                        old_stable_id,
                        current.close_reason()
                    );

                    let new_connection = self.create_connection().await;
                    *lock = Some(new_connection.clone());
                    // let old_conn = lock.replace(new_connection.clone());
                    self.reconnect_count.fetch_add(1, Ordering::SeqCst);

                    debug!(
                        "Replace closed connection {} with {} (retry {})",
                        old_stable_id,
                        new_connection.stable_id(),
                        self.reconnect_count.load(Ordering::SeqCst)
                    );

                    new_connection
                } else {
                    debug!("Reuse connection {} with write-lock", current.stable_id());
                    current.clone()
                }
            }
            None => {
                let new_connection = self.create_connection().await;

                assert!(lock.is_none(), "old connection must be None");
                *lock = Some(new_connection.clone());
                // let old_conn = foo.replace(Some(new_connection.clone()));
                debug!("Create initial connection {}", new_connection.stable_id());

                new_connection
            }
        }
    }

    async fn create_connection(&self) -> Connection {
        let connection = self
            .endpoint
            .connect(self.target_address, "localhost")
            .expect("handshake");

        connection.await.expect("connection")
    }
}

impl fmt::Display for AutoReconnect {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Connection to {}", self.target_address,)
    }
}
