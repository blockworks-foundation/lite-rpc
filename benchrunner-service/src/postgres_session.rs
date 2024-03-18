use std::env;
use std::sync::Arc;

use anyhow::Context;
use log::debug;
use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use solana_lite_rpc_util::encoding::BinaryEncoding;
use tokio::sync::RwLock;
use tokio_postgres::{
    config::SslMode, tls::MakeTlsConnect, types::ToSql, Client, CopyInSink, Error, NoTls, Row,
    Socket,
};

#[derive(serde::Deserialize, Debug, Clone)]
pub struct PostgresSessionConfig {
    pub pg_config: String,
    pub ssl: Option<PostgresSessionSslConfig>,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct PostgresSessionSslConfig {
    pub ca_pem_b64: String,
    pub client_pks_b64: String,
    pub client_pks_pass: String,
}


impl PostgresSessionConfig {
    pub fn new_from_env() -> anyhow::Result<Option<Self>> {
        // pg not enabled
        if env::var("PG_ENABLED").is_err() {
            return Ok(None);
        }

        let enable_pg = env::var("PG_ENABLED").context("PG_ENABLED")?;
        if enable_pg != *"true" {
            return Ok(None);
        }

        let env_pg_config = env::var("PG_CONFIG").context("PG_CONFIG not found")?;

        let ssl_config = if env_pg_config
            .parse::<tokio_postgres::Config>()?
            .get_ssl_mode()
            .eq(&SslMode::Disable)
        {
            None
        } else {
            let env_ca_pem_b64 = env::var("CA_PEM_B64").context("CA_PEM_B64 not found")?;
            let env_client_pks_b64 =
                env::var("CLIENT_PKS_B64").context("CLIENT_PKS_B64 not found")?;
            let env_client_pks_pass =
                env::var("CLIENT_PKS_PASS").context("CLIENT_PKS_PASS not found")?;

            Some(PostgresSessionSslConfig {
                ca_pem_b64: env_ca_pem_b64,
                client_pks_b64: env_client_pks_b64,
                client_pks_pass: env_client_pks_pass,
            })
        };

        Ok(Some(Self {
            pg_config: env_pg_config,
            ssl: ssl_config,
        }))
    }
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
}
