mod prometheus_sync;

use std::net::SocketAddr;
use std::ops::AddAssign;
use std::str::FromStr;
use std::time::Duration;
use log::info;
use prometheus::{Gauge, IntGauge, opts, register_gauge, register_int_gauge};
use tokio_cron_scheduler::{Job, JobScheduler, JobSchedulerError};
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

    let prometheus_task = PrometheusSync::sync(SocketAddr::from_str("[::]:9091").unwrap());

    let mut sched = JobScheduler::new().await?;

    // Add basic cron job
    sched.add(
        Job::new_async("1/2 * * * * *", |_uuid, _l| Box::pin(work()))?
    ).await?;


    sched.start().await?;

    // Wait while the jobs run
    tokio::time::sleep(Duration::from_secs(100)).await;


    Ok(())
}

async fn work() {


    info!("I run every 2 seconds");

    PROM_TX_SENT.set(80);

}
