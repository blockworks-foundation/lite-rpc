use bench::metrics::Metric;

use crate::args::TenantConfig;
use bench::service_adapter::BenchConfig;
use prometheus::{opts, register_gauge_vec, register_int_gauge_vec, GaugeVec, IntGaugeVec};

// https://github.com/blockworks-foundation/lite-rpc/blob/production/bench/src/metrics.rs
lazy_static::lazy_static! {
    static ref PROM_TXS_SENT: IntGaugeVec = register_int_gauge_vec!(opts!("literpc_benchrunner_txs_sent", "Total number of transactions sent"), &["tenant"]).unwrap();
    static ref PROM_TXS_CONFIRMED: IntGaugeVec = register_int_gauge_vec!(opts!("literpc_benchrunner_txs_confirmed", "Number of transactions confirmed"), &["tenant"]).unwrap();
    static ref PROM_TXS_UN_CONFIRMED: IntGaugeVec = register_int_gauge_vec!(opts!("literpc_benchrunner_txs_un_confirmed", "Number of transactions not confirmed"), &["tenant"]).unwrap();
    static ref PROM_AVG_CONFIRM: GaugeVec = register_gauge_vec!(opts!("literpc_benchrunner_avg_confirmation_time", "Confirmation time(ms)"), &["tenant"]).unwrap();
    // static ref RPC_RESPONDING: Gauge = register_gauge!(opts!("literpc_benchrunner_send_tps", "Transactions")).unwrap();
    // TODO add more
}

pub async fn publish_metrics_on_prometheus(
    tenant_config: &TenantConfig,
    _bench_config: &BenchConfig,
    metric: &Metric,
) {
    let dimensions: &[&str] = &[&tenant_config.tenant_id];

    PROM_TXS_SENT
        .with_label_values(dimensions)
        .set(metric.txs_sent as i64);
    PROM_TXS_CONFIRMED
        .with_label_values(dimensions)
        .set(metric.txs_confirmed as i64);
    PROM_TXS_UN_CONFIRMED
        .with_label_values(dimensions)
        .set(metric.txs_un_confirmed as i64);
    PROM_AVG_CONFIRM
        .with_label_values(dimensions)
        .set(metric.average_confirmation_time_ms);
}
