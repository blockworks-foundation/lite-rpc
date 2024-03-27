use crate::args::TenantConfig;
use crate::postgres::postgres_session_cache::PostgresSessionCache;
use bench::metrics::Metric;
use bench::service_adapter1::BenchConfig;
use log::warn;
use postgres_types::ToSql;
use std::time::SystemTime;
use async_trait::async_trait;
use crate::{BenchMetricsPostgresSaver, BenchRunner, BenchRunnerOldBenchImpl};

#[allow(clippy::upper_case_acronyms)]
pub enum BenchRunStatus {
    STARTED,
    FINISHED,
}

impl BenchRunStatus {
    pub fn to_db_string(&self) -> &str {
        match self {
            BenchRunStatus::STARTED => "STARTED",
            BenchRunStatus::FINISHED => "FINISHED",
        }
    }
}

pub async fn upsert_benchrun_status(
    postgres_session: &PostgresSessionCache,
    tenant_config: &TenantConfig,
    _bench_config: &BenchConfig,
    benchrun_at: SystemTime,
    status: BenchRunStatus,
) -> anyhow::Result<()> {
    let values: &[&(dyn ToSql + Sync)] = &[
        &tenant_config.tenant_id,
        &benchrun_at,
        &status.to_db_string(),
    ];
    let write_result = postgres_session
        .get_session()
        .await?
        .execute(
            r#"
            INSERT INTO benchrunner.bench_runs (
                tenant,
                ts,
                status
             )
            VALUES ($1, $2, $3)
            ON CONFLICT (tenant, ts) DO UPDATE SET status = $3
        "#,
            values,
        )
        .await;

    if let Err(err) = write_result {
        warn!("Failed to upsert status (err {:?}) - continue", err);
    }

    Ok(())
}



#[async_trait]
impl BenchMetricsPostgresSaver<Metric> for BenchRunnerOldBenchImpl {
    async fn try_save_results_postgres(&self, metric: &Metric, postgres_session: &PostgresSessionCache) -> anyhow::Result<()> {
        let metricjson = serde_json::to_value(metric).unwrap();
        let values: &[&(dyn ToSql + Sync)] = &[
            &self.tenant_config.tenant_id,
            &self.benchrun_at,
            &(self.bench_config.cu_price_micro_lamports as i64),
            &(metric.txs_sent as i64),
            &(metric.txs_confirmed as i64),
            &(metric.txs_un_confirmed as i64),
            &(metric.average_confirmation_time_ms as f32),
            &metricjson,
        ];
        postgres_session
            .get_session()
            .await?
            .execute(
                r#"
            INSERT INTO
            benchrunner.bench_metrics (
                tenant,
                ts,
                prio_fees,
                txs_sent,
                txs_confirmed, txs_un_confirmed,
                average_confirmation_time_ms,
                metric_json
             )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
                values,
            )
            .await?;


        Ok(())
    }
}


