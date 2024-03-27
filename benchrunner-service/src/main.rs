mod args;
mod cli;
mod postgres;
mod prometheus;

use crate::args::{get_funded_payer_from_env, read_tenant_configs, TenantConfig};
use crate::cli::Args;
use crate::postgres::metrics_dbstore::{
    upsert_benchrun_status, BenchRunStatus,
};
use crate::postgres::postgres_session::PostgresSessionConfig;
use crate::postgres::postgres_session_cache::PostgresSessionCache;
use crate::prometheus::metrics_prometheus::publish_metrics_on_prometheus;
use crate::prometheus::prometheus_sync::PrometheusSync;
use bench::service_adapter1::BenchConfig;
use clap::Parser;
use futures_util::future::join_all;
use itertools::Itertools;
use log::{debug, error, info, warn};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use async_trait::async_trait;
use postgres_types::ToSql;
use solana_sdk::signature::Keypair;
use bench::metrics::Metric;
use bench::tx_size::TxSize;

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

    let funded_payer = Arc::new(get_funded_payer_from_env());

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
    // let postgres_session = Arc::new(PostgresSession::new(postgres_config.unwrap()).await);

    let postgres_session = match postgres_config {
        None => None,
        Some(x) => {
            let session_cache = PostgresSessionCache::new(x)
                .await
                .expect("PostgreSQL session cache");
            Some(session_cache)
        }
    };

    let bench_configs = prio_fees
        .iter()
        .map(|prio_fees| BenchConfig {
            tx_count,
            cu_price_micro_lamports: *prio_fees,
        })
        .collect_vec();

    // 1 task per tenant - each task will perform bench runs in sequence
    // (the slot comparison bench is done somewhere else)
    for tenant_config in &tenant_configs {
        let funded_payer = funded_payer.clone();
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

                let bench_impl = BenchRunnerOldBenchImpl {
                    benchrun_at,
                    tenant_config: tenant_config.clone(),
                    bench_config: bench_config.clone(),
                    funded_payer: funded_payer.clone(),
                    size_tx,
                };

                if let Some(postgres_session) = postgres_session.as_ref() {
                    let _dbstatus = upsert_benchrun_status(
                        postgres_session,
                        &tenant_config,
                        &bench_config,
                        benchrun_at,
                        BenchRunStatus::STARTED,
                    )
                    .await;
                }

                let metric = bench_impl.run_bench().await;
                // let metric = bench::service_adapter1::bench_servicerunner(
                //     &bench_config,
                //     tenant_config.rpc_addr.clone(),
                //     funded_payer.insecure_clone(),
                //     size_tx,
                // )
                // .await;

                if let Some(postgres_session) = postgres_session.as_ref() {
                    // let _dbstatus = save_metrics_to_postgres(
                    //     postgres_session,
                    //     &tenant_config,
                    //     &bench_config,
                    //     &metric,
                    //     benchrun_at,
                    // )
                    // .await;
                    let save_result = bench_impl.try_save_results_postgres(&metric, postgres_session).await;
                    if let Err(err) = save_result {
                        warn!("Failed to save metrics to postgres (err {:?}) - continue", err);
                    }

                }

                publish_metrics_on_prometheus(&tenant_config, &bench_config, &metric).await;

                if let Some(postgres_session) = postgres_session.as_ref() {
                    let _dbstatus = upsert_benchrun_status(
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

// R: result
#[async_trait]
trait BenchRunner<M> {
    async fn run_bench(&self) -> M;
}

// R: result
#[async_trait]
trait BenchMetricsPostgresSaver<M> {
    async fn try_save_results_postgres(&self, metric: &M, postgres_session: &PostgresSessionCache) -> anyhow::Result<()>;
}

struct BenchRunnerOldBenchImpl {
    pub benchrun_at: SystemTime,
    pub tenant_config: TenantConfig,
    pub bench_config: BenchConfig,
    pub funded_payer: Arc<Keypair>,
    pub size_tx: TxSize,
}

#[async_trait]
impl BenchRunner<Metric> for BenchRunnerOldBenchImpl {
    async fn run_bench(&self) -> Metric {
        bench::service_adapter1::bench_servicerunner(
            &self.bench_config,
            self.tenant_config.rpc_addr.clone(),
            self.funded_payer.insecure_clone(),
            self.size_tx,
        )
        .await
    }
}
