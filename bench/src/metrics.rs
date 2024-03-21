use std::{
    fmt::{self, Display},
    ops::{AddAssign, DivAssign},
    time::Duration,
};

use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use solana_sdk::{signature::Signature, slot_history::Slot};
use tracing::debug;

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct Metric {
    pub txs_sent: u64,
    pub txs_confirmed: u64,
    pub txs_un_confirmed: u64,
    pub average_confirmation_time_ms: f64,
    pub average_time_to_send_txs: f64,
    pub average_transaction_bytes: f64,
    pub send_tps: f64,

    #[serde(skip_serializing)]
    total_sent_time: Duration,
    #[serde(skip_serializing)]
    total_transaction_bytes: u64,
    #[serde(skip_serializing)]
    total_confirmation_time: Duration,
    #[serde(skip_serializing)]
    total_gross_send_time_ms: f64,
}

impl Metric {
    pub fn add_successful_transaction(
        &mut self,
        time_to_send: Duration,
        time_to_confrim: Duration,
        transaction_bytes: u64,
    ) {
        self.total_sent_time += time_to_send;
        self.total_confirmation_time += time_to_confrim;
        self.total_transaction_bytes += transaction_bytes;

        self.txs_confirmed += 1;
        self.txs_sent += 1;
    }

    pub fn add_unsuccessful_transaction(&mut self, time_to_send: Duration, transaction_bytes: u64) {
        self.total_sent_time += time_to_send;
        self.total_transaction_bytes += transaction_bytes;
        self.txs_un_confirmed += 1;
        self.txs_sent += 1;
    }

    pub fn finalize(&mut self) {
        if self.txs_sent > 0 {
            self.average_time_to_send_txs =
                self.total_sent_time.as_millis() as f64 / self.txs_sent as f64;
            self.average_transaction_bytes =
                self.total_transaction_bytes as f64 / self.txs_sent as f64;
        }

        if self.total_gross_send_time_ms > 0.01 {
            let total_gross_send_time_secs = self.total_gross_send_time_ms / 1_000.0;
            self.send_tps = self.txs_sent as f64 / total_gross_send_time_secs;
        }

        if self.txs_confirmed > 0 {
            self.average_confirmation_time_ms =
                self.total_confirmation_time.as_millis() as f64 / self.txs_confirmed as f64;
        }
    }

    pub fn set_total_gross_send_time(&mut self, total_gross_send_time_ms: f64) {
        self.total_gross_send_time_ms = total_gross_send_time_ms;
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
        self.total_transaction_bytes += rhs.total_transaction_bytes;
        self.total_gross_send_time_ms += rhs.total_gross_send_time_ms;
        self.send_tps += rhs.send_tps;

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
        self.total_transaction_bytes = self.total_transaction_bytes / rhs;
        self.send_tps = self.send_tps / rhs as f64;
        self.total_gross_send_time_ms = self.total_gross_send_time_ms / rhs as f64;

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

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct TxMetricData {
    pub signature: String,
    pub sent_slot: Slot,
    pub confirmed_slot: Slot,
    pub time_to_send_in_millis: u64,
    pub time_to_confirm_in_millis: u64,
}

#[derive(Clone, Debug)]
pub enum PingThingCluster {
    Mainnet,
    Testnet,
    Devnet,
}

impl PingThingCluster {
    pub fn from_arg(cluster: String) -> Self {
        match cluster.to_lowercase().as_str() {
            "mainnet" => PingThingCluster::Mainnet,
            "testnet" => PingThingCluster::Testnet,
            "devnet" => PingThingCluster::Devnet,
            _ => panic!("incorrect cluster name"),
        }
    }
}

impl PingThingCluster {
    pub fn to_url_part(&self) -> String {
        match self {
            PingThingCluster::Mainnet => "mainnet",
            PingThingCluster::Testnet => "testnet",
            PingThingCluster::Devnet => "devnet",
        }
        .to_string()
    }
}

#[derive(Clone, Debug)]
pub enum PingThingTxType {
    Transfer,
    Memo,
}

impl Display for PingThingTxType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PingThingTxType::Transfer => write!(f, "transfer"),
            PingThingTxType::Memo => write!(f, "memo"),
        }
    }
}

#[derive(Clone)]
pub struct PingThing {
    pub cluster: PingThingCluster,
    pub va_api_key: String,
}

/// request format see https://github.com/Block-Logic/ping-thing-client/blob/4c008c741164702a639c282f1503a237f7d95e64/ping-thing-client.mjs#L160
#[derive(Debug, Serialize, Deserialize)]
struct PingThingData {
    pub time: u128,
    pub signature: String,        // Tx sig
    pub transaction_type: String, // 'transfer',
    pub success: bool,            // txSuccess
    pub application: String,      // e.g. 'web3'
    pub commitment_level: String, // e.g. 'confirmed'
    pub slot_sent: Slot,
    pub slot_landed: Slot,
}

impl PingThing {
    pub async fn submit_confirmed_stats(
        &self,
        tx_elapsed: Duration,
        tx_sig: Signature,
        tx_type: PingThingTxType,
        tx_success: bool,
        slot_sent: Slot,
        slot_landed: Slot,
    ) -> anyhow::Result<()> {
        submit_stats_to_ping_thing(
            self.cluster.clone(),
            self.va_api_key.clone(),
            tx_elapsed,
            tx_sig,
            tx_type,
            tx_success,
            slot_sent,
            slot_landed,
        )
        .await
    }
}

/// submits to https://www.validators.app/ping-thing?network=mainnet
/// Assumes that the txn was sent on Mainnet and had the "confirmed" commitment level
#[allow(clippy::too_many_arguments)]
async fn submit_stats_to_ping_thing(
    cluster: PingThingCluster,
    va_api_key: String,
    tx_elapsed: Duration,
    tx_sig: Signature,
    tx_type: PingThingTxType,
    tx_success: bool,
    slot_sent: Slot,
    slot_landed: Slot,
) -> anyhow::Result<()> {
    let submit_data_request = PingThingData {
        time: tx_elapsed.as_millis(),
        signature: tx_sig.to_string(),
        transaction_type: tx_type.to_string(),
        success: tx_success,
        application: "LiteRPC.bench".to_string(),
        commitment_level: "confirmed".to_string(),
        slot_sent,
        slot_landed,
    };

    let client = reqwest::Client::new();
    // cluster: 'mainnet'
    let response = client
        .post(format!(
            "https://www.validators.app/api/v1/ping-thing/{}",
            cluster.to_url_part()
        ))
        .header("Content-Type", "application/json")
        .header("Token", va_api_key)
        .json(&submit_data_request)
        .send()
        .await?
        .error_for_status()?;

    assert_eq!(response.status(), StatusCode::CREATED);

    debug!("Sent data for tx {} to ping-thing server", tx_sig);
    Ok(())
}

#[ignore]
#[tokio::test]
async fn test_ping_thing() {
    let token = "".to_string();
    assert!(token.is_empty(), "Empty token for ping thing test");

    let ping_thing = PingThing {
        cluster: PingThingCluster::Mainnet,
        va_api_key: token,
    };

    ping_thing
        .submit_confirmed_stats(
            Duration::from_secs(2),
            Signature::new_unique(),
            PingThingTxType::Transfer,
            true,
            123,
            124,
        )
        .await
        .unwrap();
}
