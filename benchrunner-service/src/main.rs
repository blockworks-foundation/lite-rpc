mod prometheus_sync;
mod postgres_session;

use std::net::SocketAddr;
use std::ops::AddAssign;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use log::{info, trace, warn};
use prometheus::{Gauge, IntGauge, opts, register_gauge, register_int_gauge};
use tokio::join;
use tokio::sync::mpsc::Sender;
use tokio_cron_scheduler::{Job, JobScheduler, JobSchedulerError};
use tokio_postgres::types::ToSql;
use bench::create_memo_tx;
use bench::metrics::Metric;
use crate::postgres_session::{PostgresSession, PostgresSessionConfig};
use crate::prometheus_sync::PrometheusSync;


// https://github.com/blockworks-foundation/lite-rpc/blob/production/bench/src/metrics.rs
lazy_static::lazy_static! {
    static ref PROM_TXS_SENT: IntGauge = register_int_gauge!(opts!("literpc_benchrunner_txs_sent", "Total number of transactions sent")).unwrap();
    static ref PROM_TXS_CONFIRMED: IntGauge = register_int_gauge!(opts!("literpc_benchrunner_txs_confirmed", "Number of transactions confirmed")).unwrap();
    static ref PROM_TXS_UN_CONFIRMED: IntGauge = register_int_gauge!(opts!("literpc_benchrunner_txs_un_confirmed", "Number of transactions not confirmed")).unwrap();
    static ref PROM_AVG_CONFIRM: Gauge = register_gauge!(opts!("literpc_benchrunner_avg_confirmation_time", "Confirmation time(ms)")).unwrap();
    // static ref RPC_RESPONDING: Gauge = register_gauge!(opts!("literpc_benchrunner_send_tps", "Transactions")).unwrap();
    // TODO add more
}

#[tokio::main]
async fn main() -> Result<(), JobSchedulerError> {
    tracing_subscriber::fmt::init();

    // TODO make it optional
    let postgres_config = PostgresSessionConfig::new_from_env().unwrap();
    let postgres_session = Arc::new(PostgresSession::new(postgres_config.unwrap()).await.unwrap());

    let _prometheus_task = PrometheusSync::sync(SocketAddr::from_str("[::]:9091").unwrap());


    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {

            work(postgres_session.clone()).await;

            interval.tick().await;
        }
    });

    // Wait while the jobs run
    tokio::time::sleep(Duration::from_secs(100)).await;


    Ok(())
}

async fn work(postgres_session: Arc<PostgresSession>) {
    info!("I run every 2 seconds");

    let metric = bench::service_adapter::bench_servicerunner().await;

    // Metric { txs_sent: 10, txs_confirmed: 10, txs_un_confirmed: 0,
    // average_confirmation_time_ms: 3001.8, average_time_to_send_txs: 31.9,
    // average_transaction_bytes: 179.0, send_tps: 30.408168850479996,
    // total_sent_time: 319.813167ms, total_transaction_bytes: 1790,
    // total_confirmation_time: 30.01841329s,
    // total_gross_send_time_ms: 328.859 }
    trace!("bench metric: {:?}", metric);

    join!(
        save_metrics_to_postgres(postgres_session.clone(), &metric),
        publish_metrics_on_prometheus(&metric)
    );

}

async fn save_metrics_to_postgres(postgres_session: Arc<PostgresSession>, metric: &Metric) {
    let metric_timestamp = SystemTime::now();
    let metricjson = serde_json::to_value(&metric).unwrap();
    let values: &[&(dyn ToSql + Sync)] =
        &[
            &metric_timestamp,
            &(metric.txs_sent as i64),
            &(metric.txs_confirmed as i64),
            &(metric.txs_un_confirmed as i64),
            &(metric.average_confirmation_time_ms as f32),
            &metricjson,
        ];
    let write_result = postgres_session.execute(
        r#"
            INSERT INTO
            benchrunner.bench_metrics (
                ts,
                txs_sent,
                txs_confirmed, txs_un_confirmed,
                average_confirmation_time_ms,
                metric_json
             )
            VALUES ($1, $2, $3, $4, $5, $6)
        "#, values).await;


    if let Err(err) = write_result {
        warn!("Failed to insert metrics (err {:?}) - continue", err);
        return;
    }
}

async fn publish_metrics_on_prometheus(metric: &Metric) {
    PROM_TXS_SENT.set(metric.txs_sent as i64);
    PROM_TXS_CONFIRMED.set(metric.txs_confirmed as i64);
    PROM_TXS_UN_CONFIRMED.set(metric.txs_un_confirmed as i64);
    PROM_AVG_CONFIRM.set(metric.average_confirmation_time_ms);
}
