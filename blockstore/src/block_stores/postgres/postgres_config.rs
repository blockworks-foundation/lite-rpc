use anyhow::Context;
use std::env;
use tokio_postgres::config::SslMode;

#[derive(serde::Deserialize, Debug, Clone)]
pub struct BlockstorePostgresSessionConfig {
    pub pg_config: String,
    pub ssl: Option<PostgresSessionSslConfig>,
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct PostgresSessionSslConfig {
    pub ca_pem_b64: String,
    pub client_pks_b64: String,
    pub client_pks_pass: String,
}

impl BlockstorePostgresSessionConfig {
    // var_prefix is typically "BLOCKSTOREDB" or similar
    pub fn new_from_env(var_prefix: &str) -> anyhow::Result<Self> {
        assert!(var_prefix.len()>0, "var_prefix must not be empty");
        let env_pg_config =
            env::var(format!("{var_prefix}_PG_CONFIG")).context("pg config on env not found")?;

        let ssl_config = if env_pg_config
            .parse::<tokio_postgres::Config>()?
            .get_ssl_mode()
            .eq(&SslMode::Disable)
        {
            None
        } else {
            let env_ca_pem_b64 =
                env::var(format!("{var_prefix}_CA_PEM_B64")).context("env variable not set")?;
            let env_client_pks_b64 = env::var("{var_prefix}_CLIENT_PKS_B64")
                .context(format!("{var_prefix}_CLIENT_PKS_B64 not found"))?;
            let env_client_pks_pass = env::var("{var_prefix}_CLIENT_PKS_PASS")
                .context(format!("{var_prefix}_CLIENT_PKS_PASS not found"))?;

            Some(PostgresSessionSslConfig {
                ca_pem_b64: env_ca_pem_b64,
                client_pks_b64: env_client_pks_b64,
                client_pks_pass: env_client_pks_pass,
            })
        };

        Ok(Self {
            pg_config: env_pg_config,
            ssl: ssl_config,
        })
    }
}

impl BlockstorePostgresSessionConfig {
    pub fn new_for_tests() -> BlockstorePostgresSessionConfig {
        assert!(
            env::var("PG_CONFIG").is_err(),
            "MUST NOT provide PG_CONFIG environment variables as they are ignored!"
        );

        // see localdev_integrationtest.sql how to setup the database
        BlockstorePostgresSessionConfig {
            pg_config: r#"
            host=localhost
            dbname=literpc_integrationtest_localdev
            user=literpc_integrationtest
            password=youknowme
            sslmode=disable
            "#
            .to_string(),
            ssl: None,
        }
    }
}
