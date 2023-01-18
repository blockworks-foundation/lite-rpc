use anyhow::bail;
use postgres_openssl::MakeTlsConnector;
use tokio::task::JoinHandle;
use tokio_postgres::Client;

use openssl::ssl::{SslConnector, SslMethod};

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
        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
        builder.set_ca_file("ca.pem")?;
//        builder.set_private_key("client-key.pem")?;

        let connector = MakeTlsConnector::new(builder.build());
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
                    r#"INSERT INTO Metrics 
                        (txs_sent, txs_confirmed, txs_finalized, transactions_per_second,  confirmations_per_second,  finalized_per_second,  memory_used)
                        VALUES 
                        ($1, $2, $3, $4, $5, $6)
                    "#,
                    &[&(txs_sent as u32), &(txs_confirmed as u32), &(txs_finalized as u32), &(txs_ps as u32), &(txs_confirmed_ps as u32), &(txs_finalized_ps as u32), &(mem_used.unwrap_or_default() as u32)],
                )
                .await?;

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
