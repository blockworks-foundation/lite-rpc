use std::{
    ops::{AddAssign, DivAssign},
    time::Duration,
};

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct Metric {
    pub txs_sent: u64,
    pub txs_confirmed: u64,
    pub txs_un_confirmed: u64,
    pub average_confirmation_time_ms: f64,
    pub average_time_to_send_txs: f64,

    #[serde(skip_serializing)]
    total_sent_time: Duration,
    #[serde(skip_serializing)]
    total_confirmation_time: Duration,
}

impl Metric {
    pub fn add_successful_transaction(
        &mut self,
        time_to_send: Duration,
        time_to_confrim: Duration,
    ) {
        self.total_sent_time += time_to_send;
        self.total_confirmation_time += time_to_confrim;

        self.txs_confirmed += 1;
        self.txs_sent += 1;
    }

    pub fn add_unsuccessful_transaction(&mut self, time_to_send: Duration) {
        self.total_sent_time += time_to_send;
        self.txs_un_confirmed += 1;
        self.txs_sent += 1;
    }

    pub fn finalize(&mut self) {
        if self.txs_sent > 0 {
            self.average_time_to_send_txs =
                self.total_sent_time.as_millis() as f64 / self.txs_sent as f64;
        }

        if self.txs_confirmed > 0 {
            self.average_confirmation_time_ms =
                self.total_confirmation_time.as_millis() as f64 / self.txs_confirmed as f64;
        }
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
        self.txs_confirmed += rhs.txs_confirmed;
        self.txs_un_confirmed += rhs.txs_un_confirmed;

        self.total_confirmation_time += rhs.total_confirmation_time;
        self.total_sent_time += rhs.total_sent_time;
        self.finalize();
    }
}

impl DivAssign<u64> for Metric {
    // used to avg metrics, if there were no runs then benchmark averages across 0 runs
    fn div_assign(&mut self, rhs: u64) {
        if rhs == 0 {
            return;
        }
        self.txs_sent /= rhs;
        self.txs_confirmed /= rhs;
        self.txs_un_confirmed /= rhs;

        self.total_confirmation_time =
            Duration::from_micros((self.total_confirmation_time.as_micros() / rhs as u128) as u64);
        self.total_sent_time =
            Duration::from_micros((self.total_sent_time.as_micros() / rhs as u128) as u64);
        self.finalize();
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
