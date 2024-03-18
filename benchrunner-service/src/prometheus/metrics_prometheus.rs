use bench::metrics::Metric;

use prometheus::{Gauge, IntGauge, opts, register_gauge, register_int_gauge};


// https://github.com/blockworks-foundation/lite-rpc/blob/production/bench/src/metrics.rs
lazy_static::lazy_static! {
    static ref PROM_TXS_SENT: IntGauge = register_int_gauge!(opts!("literpc_benchrunner_txs_sent", "Total number of transactions sent")).unwrap();
    static ref PROM_TXS_CONFIRMED: IntGauge = register_int_gauge!(opts!("literpc_benchrunner_txs_confirmed", "Number of transactions confirmed")).unwrap();
    static ref PROM_TXS_UN_CONFIRMED: IntGauge = register_int_gauge!(opts!("literpc_benchrunner_txs_un_confirmed", "Number of transactions not confirmed")).unwrap();
    static ref PROM_AVG_CONFIRM: Gauge = register_gauge!(opts!("literpc_benchrunner_avg_confirmation_time", "Confirmation time(ms)")).unwrap();
    // static ref RPC_RESPONDING: Gauge = register_gauge!(opts!("literpc_benchrunner_send_tps", "Transactions")).unwrap();
    // TODO add more
}

pub async fn publish_metrics_on_prometheus(metric: &Metric) {
    PROM_TXS_SENT.set(metric.txs_sent as i64);
    PROM_TXS_CONFIRMED.set(metric.txs_confirmed as i64);
    PROM_TXS_UN_CONFIRMED.set(metric.txs_un_confirmed as i64);
    PROM_AVG_CONFIRM.set(metric.average_confirmation_time_ms);
}
