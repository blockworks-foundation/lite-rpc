mod prometheus_sync;
mod postgres_session;

use std::net::SocketAddr;
use std::ops::AddAssign;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use itertools::Itertools;
use log::{debug, info, trace, warn};
use prometheus::{Gauge, IntGauge, opts, register_gauge, register_int_gauge};
use tokio::join;
use tokio::sync::mpsc::Sender;
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
async fn main() {
    tracing_subscriber::fmt::init();

    // TODO make it optional
    let postgres_config = PostgresSessionConfig::new_from_env().unwrap();
    let bench_interval = Duration::from_secs(10);

    info!("Start running benchmarks every {:?}", bench_interval);

    let _prometheus_task = PrometheusSync::sync(SocketAddr::from_str("[::]:9091").unwrap());

    let jh_runner = tokio::spawn(async move {
        let mut interval = tokio::time::interval(bench_interval);
        let postgres_session = PostgresSession::new(postgres_config.unwrap()).await;
        for run_count in 1.. {
            info!("Invoke bench execution (#{})..", run_count);
            let benchrun_at = SystemTime::now();

            let metric = bench::service_adapter::bench_servicerunner().await;

            if let Ok(postgres_session) = &postgres_session {
                save_metrics_to_postgres(postgres_session, &metric, benchrun_at).await;
            }

            publish_metrics_on_prometheus(&metric).await;

            interval.tick().await;
        }
    });

    jh_runner.await.expect("benchrunner must not fail");

}

async fn save_metrics_to_postgres(
    postgres_session: &PostgresSession, metric: &Metric, benchrun_at: SystemTime) {
    let metricjson = serde_json::to_value(&metric).unwrap();
    let values: &[&(dyn ToSql + Sync)] =
        &[
            &benchrun_at,
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
