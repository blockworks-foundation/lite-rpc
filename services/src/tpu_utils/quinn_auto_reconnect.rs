use std::cell::RefCell;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::{debug, info};
use quinn::{Connection, Endpoint};
use tokio::sync::{RwLock, RwLockWriteGuard};

pub struct AutoReconnect {
    endpoint: Endpoint,
    // note: no read lock is used ATM
    current: RwLock<Option<Connection>>,
    target_address: SocketAddr,
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

    pub async fn roundtrip(&self, payload: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        // TOOD do smart error handling + reconnect
        // self.refresh().await.open_bi().await.unwrap()
        let (mut send_stream, recv_stream) = self.refresh().await.open_bi().await?;
        send_stream.write_all(payload.as_slice()).await?;
        send_stream.finish().await?;

        let answer = recv_stream.read_to_end(64 * 1024).await?;

        Ok(answer)
    }

    pub async fn send(&self, payload: Vec<u8>) -> anyhow::Result<()> {
        // TOOD do smart error handling + reconnect
        let mut send_stream = self.refresh().await.open_uni().await?;
        send_stream.write_all(payload.as_slice()).await?;
        send_stream.finish().await?;


        Ok(())
    }


    pub async fn refresh(&self) -> Connection {
        {
            let lock = self.current.read().await;
            let maybe_conn: &Option<Connection> = &*lock;
            if maybe_conn.as_ref().filter(|conn| conn.close_reason().is_none()).is_some() {
                // let reuse = lock.unwrap().clone();
                let reuse = maybe_conn.as_ref().unwrap();
                debug!("Reuse connection {}", reuse.stable_id());
                return reuse.clone();
            }
        }
        let mut lock = self.current.write().await;
        match &*lock {
            Some(current) => {

                if current.close_reason().is_some() {
                    info!("Connection is closed for reason: {:?}", current.close_reason());
                    // TODO log

                    let new_connection = self.create_connection().await;
                    *lock = Some(new_connection.clone());
                    // let old_conn = lock.replace(new_connection.clone());
                    self.reconnect_count.fetch_add(1, Ordering::SeqCst);

                    // debug!("Replace closed connection {} with {} (retry {})",
                    //     old_conn.map(|c| c.stable_id().to_string()).unwrap_or("none".to_string()),
                    //     new_connection.stable_id(),
                    //     self.reconnect_count.load(Ordering::SeqCst));
                    // TODO log old vs new stable_id


                    return new_connection.clone();
                } else {
                    debug!("Reuse connection {} with write-lock", current.stable_id());
                    return current.clone();
                }

            }
            None => {
                let new_connection = self.create_connection().await;

                // let old_conn = lock.replace(new_connection.clone());
                // assert!(old_conn.is_none(), "old connection should be None");
                *lock = Some(new_connection.clone());
                // let old_conn = foo.replace(Some(new_connection.clone()));
                // TODO log old vs new stable_id
                debug!("Create initial connection {}", new_connection.stable_id());

                return new_connection.clone();
            }
        }
    }

    async fn create_connection(&self) -> Connection {
        let connection =
            self.endpoint.connect(self.target_address, "localhost").expect("handshake");

        connection.await.expect("connection")
    }
}

impl fmt::Display for AutoReconnect {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Connection to {}",
               self.target_address,
        )
    }
}

