mod prometheus_sync;
mod postgres_session;

use std::net::SocketAddr;
use std::ops::AddAssign;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use log::info;
use prometheus::{Gauge, IntGauge, opts, register_gauge, register_int_gauge};
use tokio::sync::mpsc::Sender;
use tokio_cron_scheduler::{Job, JobScheduler, JobSchedulerError};
use crate::postgres_session::{PostgresSession, PostgresSessionConfig};
use crate::prometheus_sync::PrometheusSync;


// https://github.com/blockworks-foundation/lite-rpc/blob/production/bench/src/metrics.rs
lazy_static::lazy_static! {
    static ref PROM_TX_SENT: IntGauge = register_int_gauge!(opts!("literpc_benchrunner_txs_sent", "Total number of transactions sent")).unwrap();
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

    let mut sched = JobScheduler::new().await?;

    // TODO check if runs are interleaved
    sched.add(
        Job::new_async("1/2 * * * * *", move |_uuid, _l| {


            Box::pin(work(postgres_session.clone()) )})?
    ).await?;


    sched.start().await?;

    // Wait while the jobs run
    tokio::time::sleep(Duration::from_secs(100)).await;


    Ok(())
}

async fn work(postgres_session: Arc<PostgresSession>) {
    info!("I run every 2 seconds");

    PROM_TX_SENT.set(80);
    PROM_TXS_CONFIRMED.set(80);
    PROM_TXS_UN_CONFIRMED.set(20);
    PROM_AVG_CONFIRM.set(2500.0);

    let row = postgres_session.client.query_one("SELECT 1", &[]).await.unwrap();
    info!("row: {:?}", row.get::<_, i32>(0));

}
