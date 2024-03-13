use std::cell::RefCell;
use std::sync::Arc;

use anyhow::Context;
use log::{debug, trace, warn};
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use prometheus::{
    exponential_buckets, histogram_opts, linear_buckets, register_histogram_vec, HistogramVec,
};
use solana_lite_rpc_core::encoding::BinaryEncoding;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tokio_postgres::{
    config::SslMode, tls::MakeTlsConnect, types::ToSql, Client, CopyInSink, Error, NoTls, Row,
    Socket,
};

use super::postgres_config::{BlockstorePostgresSessionConfig, PostgresSessionSslConfig};

lazy_static::lazy_static! {
    static ref PG_QUERY: HistogramVec =
        register_histogram_vec!
            (histogram_opts!(
                "literpc_blockstore_postgres",
                "SQL query times in blockstore::postgres_session by method"),
            &["method"]).unwrap();
}

pub struct PostgresSession(Client);

impl PostgresSession {
    pub async fn new_from_env() -> anyhow::Result<Self> {
        let pg_session_config = BlockstorePostgresSessionConfig::new_from_env("BLOCKSTOREDB")
            .expect("Blockstore PostgreSQL Configuration from env");
        PostgresSession::new(pg_session_config).await
    }

    pub async fn new(
        BlockstorePostgresSessionConfig { pg_config, ssl }: BlockstorePostgresSessionConfig,
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

        Ok(Self(client))
    }

    pub async fn new_writer(pg_config: BlockstorePostgresSessionConfig) -> anyhow::Result<Self> {
        let write_session = Self::new(pg_config).await?;
        let statement = r#"
                SET SESSION application_name='postgres-blockstore-write-session';
                -- default: 64MB
                SET SESSION maintenance_work_mem = '256MB';
                SET SESSION vacuum_freeze_min_age = 0;
                SET SESSION lock_timeout = 3000;
            "#;

        write_session.execute_multiple(statement).await.unwrap();
        Ok(write_session)
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

    pub async fn clear_session(&self) {
        // see https://www.postgresql.org/docs/current/sql-discard.html
        // CLOSE ALL -> drop potental cursors
        // RESET ALL -> we do not want (would reset work_mem)
        // DEALLOCATE -> would drop prepared statements which we do not use ATM
        // DISCARD PLANS -> we want to keep the plans
        // DISCARD SEQUENCES -> we want to keep the sequences
        let result = self
            .0
            .batch_execute(
                r#"
               DISCARD TEMP;
                CLOSE ALL;"#,
            )
            .await;
        match result {
            Ok(_) => {
                trace!("Clear postgres session");
            }
            Err(err) => {
                log::error!("Error clearing postgres session: {err}");
            }
        }
    }

    pub async fn execute(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::error::Error> {
        let _timer = PG_QUERY.with_label_values(&["execute"]).start_timer();
        self.0.execute(statement, params).await
    }

    // execute statements seperated by semicolon
    pub async fn execute_multiple(&self, statement: &str) -> Result<(), Error> {
        let _timer = PG_QUERY
            .with_label_values(&["execute_multiple"])
            .start_timer();
        self.0.batch_execute(statement).await
    }

    pub async fn execute_prepared_batch(
        &self,
        statement: &str,
        params: &Vec<Vec<&(dyn ToSql + Sync)>>,
    ) -> Result<u64, Error> {
        let _timer = PG_QUERY
            .with_label_values(&["execute_prepared_batch"])
            .start_timer();
        let prepared_stmt = self.0.prepare(statement).await?;
        let mut total_inserted = 0;
        for row in params {
            let result = self.0.execute(&prepared_stmt, row).await;
            total_inserted += result?;
        }
        Ok(total_inserted)
    }

    pub async fn execute_prepared(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, tokio_postgres::error::Error> {
        let prepared_stmt = self.0.prepare(statement).await?;
        self.0.execute(&prepared_stmt, params).await
    }

    pub async fn execute_and_return(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        let _timer = PG_QUERY
            .with_label_values(&["execute_and_return"])
            .start_timer();
        self.0.query_opt(statement, params).await
    }

    pub async fn query_opt(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        let _timer = PG_QUERY.with_label_values(&["query_opt"]).start_timer();
        self.0.query_opt(statement, params).await
    }

    pub async fn query_one(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        let _timer = PG_QUERY.with_label_values(&["query_one"]).start_timer();
        self.0.query_one(statement, params).await
    }

    pub async fn query_list(
        &self,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let _timer = PG_QUERY.with_label_values(&["query_list"]).start_timer();
        self.0.query(statement, params).await
    }

    pub async fn copy_in(&self, statement: &str) -> Result<CopyInSink<bytes::Bytes>, Error> {
        // BinaryCopyInWriter
        // https://github.com/sfackler/rust-postgres/blob/master/tokio-postgres/tests/test/binary_copy.rs
        let _timer = PG_QUERY.with_label_values(&["copy_in"]).start_timer();
        self.0.copy_in(statement).await
    }
}

#[derive(Clone)]
pub struct PostgresSessionCache {
    session: Arc<RwLock<PostgresSession>>,
    config: BlockstorePostgresSessionConfig,
}

impl PostgresSessionCache {
    pub async fn new(config: BlockstorePostgresSessionConfig) -> anyhow::Result<Self> {
        let session = PostgresSession::new(config.clone()).await?;
        Ok(Self {
            session: Arc::new(RwLock::new(session)),
            config,
        })
    }

    // TODO remove
    // pub async fn __get_session(&self) -> PostgresSession {
    // let session = self.session.read().await;

    // if session.client.is_closed() || session.client.execute(";", &[]).await.is_err() {
    //     drop(session);
    //     let session = PostgresSession::new(self.config.clone()).await
    //         .expect("should have created new postgres session");
    //     *self.session.write().await = session.clone();
    //     session
    // } else {
    //     session.clear_session().await;
    //     session.clone()
    // }
    // }
}

// #[derive(Clone)]
// pub struct BlockstorePostgresWriteSession {
//     session: Arc<RwLock<PostgresSession>>,
//     // pub pg_session_config: BlockstorePostgresSessionConfig,
// }
//
// impl BlockstorePostgresWriteSession {
//     pub async fn new_from_env() -> anyhow::Result<Self> {
//         let pg_session_config = BlockstorePostgresSessionConfig::new_from_env()
//             .expect("Blockstore PostgreSQL Configuration from env");
//         Self::new(pg_session_config).await
//     }
//
//     pub async fn new(pg_session_config: BlockstorePostgresSessionConfig) -> anyhow::Result<Self> {
//         let session = PostgresSession::new(pg_session_config.clone()).await?;
//
//         let statement = r#"
//                 SET SESSION application_name='postgres-blockstore-write-session';
//                 -- default: 64MB
//                 SET SESSION maintenance_work_mem = '256MB';
//             "#;
//
//         session.execute_multiple(statement).await.unwrap();
//
//         Ok(Self {
//             session: Arc::new(RwLock::new(session)),
//             // pg_session_config,
//         })
//     }
//
//     // pub async fn get_write_session(&self) -> PostgresSession {
//     //     let session = self.session.read().await;
//     //
//     //     if session.0.is_closed() || session.0.execute(";", &[]).await.is_err() {
//     //         drop(session);
//     //         let session = PostgresSession::new(self.pg_session_config.clone())
//     //             .await
//     //             .expect("should have created new postgres session");
//     //         let mut lock = self.session.write().await;
//     //         *lock = session.clone();
//     //         session
//     //     } else {
//     //         session.clear_session().await;
//     //         session.clone()
//     //     }
//     // }
// }

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
