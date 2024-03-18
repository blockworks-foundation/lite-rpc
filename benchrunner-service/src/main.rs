mod postgres_session;
mod prometheus;
mod postgres;

use std::net::SocketAddr;
use std::ops::AddAssign;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use itertools::Itertools;
use log::{debug, info, trace, warn};
use solana_sdk::signature::Keypair;
use tokio::join;
use tokio::sync::mpsc::Sender;
use tokio_postgres::types::ToSql;
use tracing_subscriber::filter::FilterExt;
use bench::create_memo_tx;
use bench::helpers::BenchHelper;
use bench::metrics::Metric;
use crate::postgres_session::{PostgresSession, PostgresSessionConfig};
use crate::prometheus::metrics_prometheus::publish_metrics_on_prometheus;
use crate::prometheus::prometheus_sync::PrometheusSync;


#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let postgres_config = PostgresSessionConfig::new_from_env().unwrap();
    let bench_interval = Duration::from_secs(10);

    let token: String = std::env::var("TESTNET_API_TOKEN").expect("need testnet token on env");

    let rpc_addr = format!("https://api.testnet.rpcpool.com/{}", token);

    let keypair58_string: String = std::env::var("FUNDED_PAYER_KEYPAIR58").expect("need funded payer keypair on env (variable FUNDED_PAYER_KEYPAIR58)");

    let funded_payer = Keypair::from_base58_string(&keypair58_string);

    info!("Start running benchmarks every {:?}", bench_interval);

    let _prometheus_task = PrometheusSync::sync(SocketAddr::from_str("[::]:9091").unwrap());

    let jh_runner = tokio::spawn(async move {
        let mut interval = tokio::time::interval(bench_interval);
        let postgres_session = PostgresSession::new(postgres_config.unwrap()).await;
        for run_count in 1.. {
            debug!("Invoke bench execution (#{})..", run_count);
            let benchrun_at = SystemTime::now();

            let metric = bench::service_adapter::bench_servicerunner(rpc_addr.clone(), funded_payer.insecure_clone()).await;

            if let Ok(postgres_session) = &postgres_session {
                save_metrics_to_postgres(postgres_session, &metric, benchrun_at).await;
            }

            publish_metrics_on_prometheus(&metric).await;

            debug!("Bench execution (#{}) done in {:?}", run_count, benchrun_at.elapsed());
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
