use std::sync::Arc;

use anyhow::Context;
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use solana_lite_rpc_core::encoding::BinaryEncoding;
use tokio::sync::RwLock;
use tokio_postgres::{config::SslMode, tls::MakeTlsConnect, types::ToSql, Client, NoTls, Socket};

use super::postgres_config::{PostgresSessionConfig, PostgresSessionSslConfig};

#[derive(Clone)]
pub struct PostgresSession {
    pub client: Arc<Client>,
}

impl PostgresSession {
    pub async fn new(
        PostgresSessionConfig { pg_config, ssl }: PostgresSessionConfig,
    ) -> anyhow::Result<Self> {
        let pg_config = pg_config.parse::<tokio_postgres::Config>()?;

        let client = if let SslMode::Disable = pg_config.get_ssl_mode() {
            Self::spawn_connection(pg_config, NoTls).await?
        } else {
            let PostgresSessionSslConfig {
                ca_pem_b64,
                client_pks_b64,
                client_pks_pass,
            } = ssl.as_ref().unwrap();

            let ca_pem = BinaryEncoding::Base64
                .decode(ca_pem_b64)
                .context("ca pem decode")?;
            let client_pks = BinaryEncoding::Base64
                .decode(client_pks_b64)
                .context("client pks decode")?;

            let connector = TlsConnector::builder()
                .add_root_certificate(Certificate::from_pem(&ca_pem)?)
                .identity(Identity::from_pkcs12(&client_pks, client_pks_pass).context("Identity")?)
                .danger_accept_invalid_hostnames(true)
                .danger_accept_invalid_certs(true)
                .build()?;

            Self::spawn_connection(pg_config, MakeTlsConnector::new(connector)).await?
        };

        Ok(Self {
            client: Arc::new(client),
        })
    }

    async fn spawn_connection<T>(
        pg_config: tokio_postgres::Config,
        connector: T,
    ) -> anyhow::Result<Client>
    where
        T: MakeTlsConnect<Socket> + Send + 'static,
        <T as MakeTlsConnect<Socket>>::Stream: Send,
    {
        let (client, connection) = pg_config
            .connect(connector)
            .await
            .context("Connecting to Postgres failed")?;

        tokio::spawn(async move {
            log::debug!("Connecting to Postgres");

            if let Err(err) = connection.await {
                log::error!("Connection to Postgres broke {err:?}");
                return;
            }
            log::debug!("Postgres thread shutting down");
        });

        Ok(client)
    }

    pub async fn execute(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::error::Error> {
        self.client.execute(statement, params).await
    }

    pub fn values_vecvec(args: usize, rows: usize, types: &[&str]) -> String {
        let mut query = String::new();

        Self::multiline_query(&mut query, args, rows, types);

        query
    }

    pub fn multiline_query(query: &mut String, args: usize, rows: usize, types: &[&str]) {
        let mut arg_index = 1usize;
        for row in 0..rows {
            query.push('(');

            for i in 0..args {
                if row == 0 && !types.is_empty() {
                    query.push_str(&format!("(${arg_index})::{}", types[i]));
                } else {
                    query.push_str(&format!("${arg_index}"));
                }
                arg_index += 1;
                if i != (args - 1) {
                    query.push(',');
                }
            }

            query.push(')');

            if row != (rows - 1) {
                query.push(',');
            }
        }
    }
}

#[derive(Clone)]
pub struct PostgresSessionCache {
    session: Arc<RwLock<PostgresSession>>,
    config: PostgresSessionConfig,
}

impl PostgresSessionCache {
    pub async fn new(config: PostgresSessionConfig) -> anyhow::Result<Self> {
        let session = PostgresSession::new(config.clone()).await?;
        Ok(Self {
            session: Arc::new(RwLock::new(session)),
            config,
        })
    }

    pub async fn get_session(&self) -> anyhow::Result<PostgresSession> {
        let session = self.session.read().await;
        if session.client.is_closed() {
            drop(session);
            let session = PostgresSession::new(self.config.clone()).await?;
            *self.session.write().await = session.clone();
            Ok(session)
        } else {
            Ok(session.clone())
        }
    }
}
