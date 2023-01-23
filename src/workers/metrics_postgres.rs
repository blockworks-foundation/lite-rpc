use anyhow::{bail, Context};
use log::info;
use postgres_native_tls::MakeTlsConnector;
use tokio::fs;
use tokio::task::JoinHandle;
use tokio_postgres::Client;

use native_tls::{Certificate, Identity, TlsConnector};

use super::{Metrics, MetricsCapture};

pub struct MetricsPostgres {
    connection: JoinHandle<Result<(), tokio_postgres::Error>>,
    client: Client,
    metrics_capture: MetricsCapture,
}

impl MetricsPostgres {
    pub async fn new(
        metrics_capture: MetricsCapture,
        porstgres_config: &str,
    ) -> anyhow::Result<Self> {
        let connector = TlsConnector::builder()
            .add_root_certificate(Certificate::from_pem(&fs::read("ca.pem").await?)?)
            .identity(
                Identity::from_pkcs12(&fs::read("client.pks").await?, "p").context("Identity")?,
            )
            .danger_accept_invalid_hostnames(true)
            .danger_accept_invalid_certs(true)
            .build()?;

        info!("making tls config");

        let connector = MakeTlsConnector::new(connector);
        let (client, connection) = tokio_postgres::connect(porstgres_config, connector).await?;

        Ok(Self {
            connection: tokio::spawn(connection),
            client,
            metrics_capture,
        })
    }

    pub fn sync(self) -> JoinHandle<anyhow::Result<()>> {
        let mut one_second = tokio::time::interval(std::time::Duration::from_secs(1));
        let Self {
            connection,
            client,
            metrics_capture,
        } = self;

        let metrics_send: JoinHandle<anyhow::Result<()>> = tokio::spawn(async move {
            info!("Sending Metrics To Postgres");

            loop {
                let Metrics {
                    txs_sent,
                    txs_confirmed,
                    txs_finalized,
                    txs_ps,
                    txs_confirmed_ps,
                    txs_finalized_ps,
                    mem_used,
                } = metrics_capture.get_metrics().await;

                client.execute(
                        r#"INSERT INTO LiteRpcMetrics
                        (txs_sent, txs_confirmed, txs_finalized, transactions_per_second, confirmations_per_second, finalized_per_second, memory_used)
                        VALUES 
                        ($1, $2, $3, $4, $5, $6, $7)
                    "#,
                    &[&(txs_sent as i64), &(txs_confirmed as i64), &(txs_finalized as i64), &(txs_ps as i64), &(txs_confirmed_ps as i64), &(txs_finalized_ps as i64), &(mem_used.unwrap_or_default() as i64)],
                )
                .await.unwrap();

                one_second.tick().await;
            }
        });

        #[allow(unreachable_code)]
        tokio::spawn(async move {
            tokio::select! {
                r = metrics_send => {
                    bail!("Postgres metrics send thread stopped {r:?}")
                }
                r = connection => {
                    bail!("Postgres connection poll stopped {r:?}")
                }
            }

            Ok(())
        })
    }
}
