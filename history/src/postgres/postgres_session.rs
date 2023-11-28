use std::sync::Arc;

use anyhow::Context;
use log::info;
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use solana_lite_rpc_core::encoding::BinaryEncoding;
use tokio::sync::RwLock;
use tokio_postgres::{
    config::SslMode, tls::MakeTlsConnect, types::ToSql, Client, CopyInSink, Error, NoTls, Row,
    Socket,
};

use super::postgres_config::{PostgresSessionConfig, PostgresSessionSslConfig};

const MAX_QUERY_SIZE: usize = 200_000; // 0.2 mb

pub trait SchemaSize {
    const DEFAULT_SIZE: usize = 0;
    const MAX_SIZE: usize = 0;
}

pub const fn get_max_safe_inserts<T: SchemaSize>() -> usize {
    if T::DEFAULT_SIZE == 0 {
        panic!("DEFAULT_SIZE can't be 0. SchemaSize impl should override the DEFAULT_SIZE const");
    }

    MAX_QUERY_SIZE / T::DEFAULT_SIZE
}

pub const fn get_max_safe_updates<T: SchemaSize>() -> usize {
    if T::MAX_SIZE == 0 {
        panic!("MAX_SIZE can't be 0. SchemaSize impl should override the MAX_SIZE const");
    }

    MAX_QUERY_SIZE / T::MAX_SIZE
}

#[derive(Clone)]
pub struct PostgresSession {
    pub client: Arc<Client>,
}

impl PostgresSession {

    pub async fn new_from_env() -> anyhow::Result<Self> {
        let pg_session_config = PostgresSessionConfig::new_from_env()
            .expect("failed to start Postgres Client")
            .expect("Postgres not enabled (use PG_ENABLED)");
        PostgresSession::new(pg_session_config).await
    }

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
            log::info!("Connecting to Postgres");

            if let Err(err) = connection.await {
                log::error!("Connection to Postgres broke {err:?}");
                return;
            }
            log::debug!("Postgres thread shutting down");
        });

        Ok(client)
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

    pub fn values_vecvec(args: usize, rows: usize, types: &[&str]) -> String {
        let mut query = String::new();

        Self::multiline_query(&mut query, args, rows, types);

        query
    }

    // workaround: produces ((a,b,c)) while (a,b,c) would suffice
    pub fn values_vec(args: usize, types: &[&str]) -> String {
        let mut query = String::new();

        Self::multiline_query(&mut query, args, 1, types);

        query
    }

    pub async fn execute(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::error::Error> {
        self.client.execute(statement, params).await
    }

    pub async fn execute_simple(&self, statement: &str) -> Result<(), Error> {
        self.client.batch_execute(statement).await
    }

    pub async fn execute_prepared_batch(
        &self,
        statement: &str,
        params: &Vec<Vec<&(dyn ToSql + Sync)>>,
    ) -> Result<u64, Error> {
        let prepared_stmt = self.client.prepare(statement).await?;
        let mut total_inserted = 0;
        for row in params {
            let result = self.client.execute(&prepared_stmt, row).await;
            total_inserted += result?;
        }
        Ok(total_inserted)
    }

    // TODO provide an optimized version using "COPY IN" instead of "INSERT INTO" (https://trello.com/c/69MlQU6u)
    // pub async fn execute_copyin(...)

    pub async fn execute_prepared(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::error::Error> {
        let prepared_stmt = self.client.prepare(statement).await?;
        self.client.execute(&prepared_stmt, params).await
    }

    pub async fn execute_and_return(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        self.client.query_opt(statement, params).await
    }

    pub async fn query_opt(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        self.client.query_opt(statement, params).await
    }

    pub async fn query_one(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        self.client.query_one(statement, params).await
    }

    pub async fn query_list(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        self.client.query(statement, params).await
    }

    pub async fn copy_in(&self, statement: &str) -> Result<CopyInSink<bytes::Bytes>, Error> {
        // BinaryCopyInWriter
        // https://github.com/sfackler/rust-postgres/blob/master/tokio-postgres/tests/test/binary_copy.rs
        self.client.copy_in(statement).await
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

#[derive(Clone)]
pub struct PostgresWriteSession {
    session: Arc<RwLock<PostgresSession>>,
    pub pg_session_config: PostgresSessionConfig,
}

impl PostgresWriteSession {

    pub async fn new_from_env() -> anyhow::Result<Self> {
        let pg_session_config = PostgresSessionConfig::new_from_env()
            .expect("failed to start Postgres Client")
            .expect("Postgres not enabled (use PG_ENABLED)");
        Self::new(pg_session_config).await
    }

    pub async fn new(pg_session_config: PostgresSessionConfig) -> anyhow::Result<Self> {

        let session = PostgresSession::new(pg_session_config.clone()).await?;

        let statement = format!(
            r#"
                SET SESSION application_name='postgres-blockstore-write-session';
                -- default: 64MB
                SET SESSION maintenance_work_mem = '256MB';
            "#,
        );

        session.execute_simple(&statement).await.unwrap();

        Ok(Self {
            session: Arc::new(RwLock::new(session)),
            pg_session_config
        })
    }

    pub async fn get_write_session(&self) -> PostgresSession {
        let session = self.session.read().await;

        if session.client.is_closed() || session.client.execute(";", &[]).await.is_err() {
            let session = PostgresSession::new(self.pg_session_config.clone())
                .await
                .expect("should have created new postgres session");
            let mut lock = self.session.write().await;
            *lock = session.clone();
            session
        } else {
            session.clone()
        }
    }
}

#[test]
fn multiline_query_test() {
    let mut query = String::new();

    PostgresSession::multiline_query(&mut query, 3, 2, &[]);
    assert_eq!(query, "($1,$2,$3),($4,$5,$6)");
}

#[test]
fn value_query_test() {
    let values = PostgresSession::values_vecvec(3, 2, &[]);
    assert_eq!(values, "($1,$2,$3),($4,$5,$6)");
}

#[test]
fn multiline_query_test_types() {
    let mut query = String::new();

    PostgresSession::multiline_query(&mut query, 3, 2, &["text", "int", "int"]);
    assert_eq!(query, "(($1)::text,($2)::int,($3)::int),($4,$5,$6)");
}

#[test]
fn value_vecvec_test_types() {
    let values = PostgresSession::values_vecvec(3, 2, &["text", "int", "int"]);
    assert_eq!(values, "(($1)::text,($2)::int,($3)::int),($4,$5,$6)");
}

#[test]
fn value_vec_test_types() {
    let values = PostgresSession::values_vec(3, &["text", "int", "int"]);
    assert_eq!(values, "(($1)::text,($2)::int,($3)::int)");
}
