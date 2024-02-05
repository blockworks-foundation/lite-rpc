use anyhow::Context;
use std::env;
use tokio_postgres::config::SslMode;

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

