use std::ops::{AddAssign, DivAssign};

#[derive(Debug, Default, serde::Serialize)]
pub struct Metric {
    pub total_time_elapsed_sec: f64,
    pub txs_sent: u64,
    pub time_to_send_txs: f64,
    pub txs_confirmed: u64,
    pub txs_un_confirmed: u64,
    pub tps: f64,
}

#[derive(Default)]
pub struct AvgMetric {
    num_of_runs: u64,
    total_metric: Metric,
}

impl Metric {
    pub fn calc_tps(&mut self) {
        self.tps = self.txs_confirmed as f64 / self.total_time_elapsed_sec
    }
}

impl AddAssign<&Self> for Metric {
    fn add_assign(&mut self, rhs: &Self) {
        self.total_time_elapsed_sec += rhs.total_time_elapsed_sec;
        self.txs_sent += rhs.txs_sent;
        self.time_to_send_txs += rhs.time_to_send_txs;
        self.txs_confirmed += rhs.txs_confirmed;
        self.txs_un_confirmed += rhs.txs_un_confirmed;
        self.tps += rhs.tps
    }
}

impl DivAssign<u64> for Metric {
    // used to avg metrics, if there were no runs then benchmark averages across 0 runs
    fn div_assign(&mut self, rhs: u64) {
        if rhs == 0 {
            return;
        }
        self.total_time_elapsed_sec /= rhs as f64;
        self.txs_sent /= rhs;
        self.time_to_send_txs /= rhs as f64;
        self.txs_confirmed /= rhs;
        self.txs_un_confirmed /= rhs;
        self.tps /= rhs as f64;
    }
}

impl AddAssign<&Metric> for AvgMetric {
    fn add_assign(&mut self, rhs: &Metric) {
        self.num_of_runs += 1;
        self.total_metric += rhs;
    }
}

impl From<AvgMetric> for Metric {
    fn from(mut avg_metric: AvgMetric) -> Self {
        avg_metric.total_metric /= avg_metric.num_of_runs;
        avg_metric.total_metric
    }
}
