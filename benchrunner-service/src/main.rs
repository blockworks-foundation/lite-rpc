mod args;
mod cli;
mod postgres;
mod prometheus;

use crate::args::{get_funded_payer_from_env, read_tenant_configs};
use crate::cli::Args;
use crate::postgres::metrics_dbstore::{
    save_metrics_to_postgres, upsert_benchrun_status, BenchRunStatus,
};
use crate::postgres::postgres_session::{PostgresSession, PostgresSessionConfig};
use crate::prometheus::metrics_prometheus::publish_metrics_on_prometheus;
use crate::prometheus::prometheus_sync::PrometheusSync;
use bench::create_memo_tx;
use bench::helpers::BenchHelper;
use bench::metrics::{Metric, TxMetricData};
use bench::service_adapter::BenchConfig;
use clap::Parser;
use futures_util::future::join_all;
use itertools::Itertools;
use log::{debug, error, info, trace, warn};
use solana_sdk::signature::Keypair;
use std::net::SocketAddr;
use std::ops::AddAssign;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::join;
use tokio::sync::mpsc::Sender;
use tokio_postgres::types::ToSql;
use tracing_subscriber::filter::FilterExt;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let Args {
        bench_interval,
        tx_count,
        size_tx,
        prio_fees,
    } = Args::parse();

    let postgres_config = PostgresSessionConfig::new_from_env().unwrap();
    let bench_interval = Duration::from_millis(bench_interval);

    let funded_payer = get_funded_payer_from_env();

    let tenant_configs = read_tenant_configs(std::env::vars().collect::<Vec<(String, String)>>());

    info!("Use postgres config: {:?}", postgres_config.is_some());
    info!("Use prio fees: [{}]", prio_fees.iter().join(","));
    info!("Start running benchmarks every {:?}", bench_interval);
    info!(
        "Found tenants: {}",
        tenant_configs.iter().map(|tc| &tc.tenant_id).join(", ")
    );

    if tenant_configs.is_empty() {
        error!("No tenants found (missing env vars) - exit");
        return;
    }

    let _prometheus_task = PrometheusSync::sync(SocketAddr::from_str("[::]:9091").unwrap());

    let mut jh_tenant_task = Vec::new();
    let postgres_session = Arc::new(PostgresSession::new(postgres_config.unwrap()).await);

    let bench_configs = prio_fees
        .iter()
        .map(|prio_fees| BenchConfig {
            tx_count,
            cu_price_micro_lamports: *prio_fees,
        })
        .collect_vec();

    for tenant_config in &tenant_configs {
        let funded_payer = funded_payer.insecure_clone();
        let tenant_id = tenant_config.tenant_id.clone();
        let postgres_session = postgres_session.clone();
        let tenant_config = tenant_config.clone();
        let bench_configs = bench_configs.clone();
        let jh_runner = tokio::spawn(async move {
            let mut interval = tokio::time::interval(bench_interval);
            for run_count in 1.. {
                let bench_config = bench_configs[run_count % bench_configs.len()].clone();
                debug!(
                    "Invoke bench execution (#{}) on tenant <{}> using {}",
                    run_count, tenant_id, bench_config
                );
                let benchrun_at = SystemTime::now();

                if let Ok(postgres_session) = postgres_session.as_ref() {
                    upsert_benchrun_status(
                        postgres_session,
                        &tenant_config,
                        &bench_config,
                        benchrun_at,
                        BenchRunStatus::STARTED,
                    )
                    .await;
                }

                let metric = bench::service_adapter::bench_servicerunner(
                    &bench_config,
                    tenant_config.rpc_addr.clone(),
                    funded_payer.insecure_clone(),
                    size_tx,
                )
                .await;

                if let Ok(postgres_session) = postgres_session.as_ref() {
                    save_metrics_to_postgres(
                        postgres_session,
                        &tenant_config,
                        &bench_config,
                        &metric,
                        benchrun_at,
                    )
                    .await;
                }

                publish_metrics_on_prometheus(&tenant_config, &bench_config, &metric).await;

                if let Ok(postgres_session) = postgres_session.as_ref() {
                    upsert_benchrun_status(
                        postgres_session,
                        &tenant_config,
                        &bench_config,
                        benchrun_at,
                        BenchRunStatus::FINISHED,
                    )
                    .await;
                }
                debug!(
                    "Bench execution (#{}) done in {:?}",
                    run_count,
                    benchrun_at.elapsed().unwrap()
                );
                interval.tick().await;
            }
        });
        jh_tenant_task.push(jh_runner);
    } // -- END tenant loop

    join_all(jh_tenant_task).await;
}
