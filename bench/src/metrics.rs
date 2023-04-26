use std::{
    ops::{AddAssign, Div, DivAssign, Mul},
    time::Duration,
};

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct Metric {
    pub txs_sent: u64,
    pub time_to_send_txs: f64,
    pub txs_confirmed: u64,
    pub txs_un_confirmed: u64,
    pub average_confirmation_time_ms: f64,
}

impl Metric {
    pub fn add_successful_transaction(
        &mut self,
        time_to_send: Duration,
        time_to_send_and_confrim: Duration,
    ) {
        self.time_to_send_txs = (self.time_to_send_txs.mul(self.txs_confirmed as f64)
            + time_to_send.as_millis() as f64)
            .div(self.txs_confirmed as f64 + 1.0);
        self.average_confirmation_time_ms = (self
            .average_confirmation_time_ms
            .mul(self.txs_confirmed as f64)
            + time_to_send_and_confrim.as_millis() as f64)
            .div(self.txs_confirmed as f64 + 1.0);
        self.txs_confirmed += 1;
        self.txs_sent += 1;
    }

    pub fn add_unsuccessful_transaction(&mut self) {
        self.txs_un_confirmed += 1;
        self.txs_sent += 1;
    }
}

#[derive(Default)]
pub struct AvgMetric {
    num_of_runs: u64,
    total_metric: Metric,
}

impl Metric {
    pub fn calc_tps(&mut self) -> f64 {
        self.txs_confirmed as f64
    }
}

impl AddAssign<&Self> for Metric {
    fn add_assign(&mut self, rhs: &Self) {
        self.txs_sent += rhs.txs_sent;
        self.time_to_send_txs += rhs.time_to_send_txs;
        self.txs_confirmed += rhs.txs_confirmed;
        self.txs_un_confirmed += rhs.txs_un_confirmed;
        self.average_confirmation_time_ms += rhs.average_confirmation_time_ms;
    }
}

impl DivAssign<u64> for Metric {
    // used to avg metrics, if there were no runs then benchmark averages across 0 runs
    fn div_assign(&mut self, rhs: u64) {
        if rhs == 0 {
            return;
        }
        self.txs_sent /= rhs;
        self.time_to_send_txs /= rhs as f64;
        self.txs_confirmed /= rhs;
        self.txs_un_confirmed /= rhs;
        self.average_confirmation_time_ms /= rhs as f64;
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
