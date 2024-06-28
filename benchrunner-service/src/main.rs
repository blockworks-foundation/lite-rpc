mod args;
mod cli;
mod postgres;
mod prometheus;

use crate::args::{get_funded_payer_from_env, read_tenant_configs, TenantConfig};
use crate::cli::Args;
use crate::postgres::metrics_dbstore::{upsert_benchrun_status, BenchRunStatus};
use crate::postgres::postgres_session::PostgresSessionConfig;
use crate::postgres::postgres_session_cache::PostgresSessionCache;
use crate::prometheus::prometheus_sync::PrometheusSync;
use async_trait::async_trait;
use bench::benches::confirmation_rate;
use bench::metrics;
use bench::service_adapter1::BenchConfig;
use clap::Parser;
use futures_util::future::join_all;
use itertools::Itertools;
use log::{debug, error, info, warn};
use solana_sdk::signature::Keypair;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::OnceCell;

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
            tx_size: size_tx,
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
                const NUM_BENCH_IMPLS: usize = 2;

                let benchrun_at = SystemTime::now();

                let permutation = factorize(run_count, &[NUM_BENCH_IMPLS, bench_configs.len()]);

                let bench_config = bench_configs[permutation[1]].clone();

                let bench_impl: Box<dyn BenchTrait> = match permutation[0] {
                    0 => Box::new(BenchRunnerConfirmationRateImpl {
                        benchrun_at,
                        tenant_config: tenant_config.clone(),
                        bench_config: bench_config.clone(),
                        funded_payer: funded_payer.clone(),
                        metric: OnceCell::new(),
                    }),
                    1 => Box::new(BenchRunnerBench1Impl {
                        benchrun_at,
                        tenant_config: tenant_config.clone(),
                        bench_config: bench_config.clone(),
                        funded_payer: funded_payer.clone(),
                        metric: OnceCell::new(),
                    }),
                    _ => unreachable!(),
                };

                debug!(
                    "Invoke bench execution (#{}) on tenant <{}> using {}",
                    run_count, tenant_id, &bench_config
                );

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

                bench_impl.run_bench().await;

                if let Some(postgres_session) = postgres_session.as_ref() {
                    let save_result = bench_impl.try_save_results_postgres(postgres_session).await;
                    if let Err(err) = save_result {
                        warn!(
                            "Failed to save metrics to postgres (err {:?}) - continue",
                            err
                        );
                    }
                }

                // publish_metrics_on_prometheus(&tenant_config, &bench_config).await;

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

// dimensions: least-significant first
fn factorize(i: usize, dimensions: &[usize]) -> Vec<usize> {
    let mut i = i;
    let mut result = Vec::new();
    for &dim in dimensions {
        result.push(i % dim);
        i /= dim;
    }
    result
}

#[test]
fn test_factorize() {
    assert_eq!(factorize(0, &[2, 3]), vec![0, 0]);
    assert_eq!(factorize(1, &[2, 3]), vec![1, 0]);
    assert_eq!(factorize(2, &[2, 3]), vec![0, 1]);
    assert_eq!(factorize(3, &[2, 3]), vec![1, 1]);
    assert_eq!(factorize(4, &[2, 3]), vec![0, 2]);
    assert_eq!(factorize(5, &[2, 3]), vec![1, 2]);
}

// R: result
#[async_trait]
trait BenchRunner: Send + Sync + 'static {
    async fn run_bench(&self);
}

trait BenchTrait: BenchRunner + BenchMetricsPostgresSaver {}

// R: result
#[async_trait]
trait BenchMetricsPostgresSaver: Send + Sync + 'static {
    async fn try_save_results_postgres(
        &self,
        postgres_session: &PostgresSessionCache,
    ) -> anyhow::Result<()>;
}

struct BenchRunnerBench1Impl {
    pub benchrun_at: SystemTime,
    pub tenant_config: TenantConfig,
    pub bench_config: BenchConfig,
    pub funded_payer: Arc<Keypair>,
    pub metric: OnceCell<metrics::Metric>,
}

impl BenchTrait for BenchRunnerBench1Impl {}

#[async_trait]
impl BenchRunner for BenchRunnerBench1Impl {
    async fn run_bench(&self) {
        let metric = bench::service_adapter1::bench_servicerunner(
            &self.bench_config,
            self.tenant_config.rpc_addr.clone(),
            self.funded_payer.insecure_clone(),
        )
        .await;
        self.metric.set(metric).unwrap();
    }
}

impl BenchTrait for BenchRunnerConfirmationRateImpl {}

struct BenchRunnerConfirmationRateImpl {
    pub benchrun_at: SystemTime,
    pub tenant_config: TenantConfig,
    pub bench_config: BenchConfig,
    pub funded_payer: Arc<Keypair>,
    pub metric: OnceCell<confirmation_rate::Metric>,
}

#[async_trait]
impl BenchRunner for BenchRunnerConfirmationRateImpl {
    async fn run_bench(&self) {
        let metric = bench::service_adapter_new::benchnew_confirmation_rate_servicerunner(
            &self.bench_config,
            self.tenant_config.rpc_addr.clone(),
            self.funded_payer.insecure_clone(),
        )
        .await;
        self.metric.set(metric).unwrap();
    }
}
