use std::sync::Arc;
use log::info;
use tokio::sync::RwLock;
use crate::postgres::postgres_session::{PostgresSession, PostgresSessionConfig};

#[derive(Clone)]
pub struct PostgresSessionCache {
    session: Arc<RwLock<PostgresSession>>,
    config: Arc<PostgresSessionConfig>,
}

impl PostgresSessionCache {
    pub async fn new(config: PostgresSessionConfig) -> anyhow::Result<Self> {
        let session = PostgresSession::new(config.clone()).await?;
        Ok(Self {
            session: Arc::new(RwLock::new(session)),
            config: Arc::new(config),
        })
    }

    pub async fn get_session(&self) -> anyhow::Result<PostgresSession> {
        let session = self.session.read().await;
        if session.is_closed() {
            info!("PostgreSQL session closed - reconnecting");
            drop(session);
            let session = PostgresSession::new(self.config.as_ref().clone()).await?;
            *self.session.write().await = session.clone();
            Ok(session)
        } else {
            Ok(session.clone())
        }
    }
}
