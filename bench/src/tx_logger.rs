use tokio::sync::mpsc::UnboundedReceiver;

use crate::metrics::TxMetricData;

pub struct TxLogger;

impl TxLogger {
    pub async fn log(path: &str, mut rx: UnboundedReceiver<TxMetricData>) -> anyhow::Result<()> {
        let mut tx_writer = csv::Writer::from_path(path).unwrap();

        while let Some(x) = rx.recv().await {
            tx_writer.serialize(x)?;
        }

        anyhow::bail!("TxLogger exited")
    }
}
