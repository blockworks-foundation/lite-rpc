use std::time::Duration;

use anyhow::bail;
use clap::Parser;
use lite_rpc::{bridge::LiteBridge, cli::Args};
use log::info;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let Args {
        rpc_addr,
        ws_addr,
        tx_batch_size,
        lite_rpc_ws_addr,
        lite_rpc_http_addr,
        tx_batch_interval_ms,
        clean_interval_ms,
        fanout_size,
        enable_postgres,
    } = Args::parse();

    let tx_batch_interval_ms = Duration::from_millis(tx_batch_interval_ms);
    let clean_interval_ms = Duration::from_millis(clean_interval_ms);

    let light_bridge = LiteBridge::new(rpc_addr, ws_addr, fanout_size).await?;

    let services = light_bridge
        .start_services(
            lite_rpc_http_addr,
            lite_rpc_ws_addr,
            tx_batch_size,
            tx_batch_interval_ms,
            clean_interval_ms,
            enable_postgres,
        )
        .await?;

    let services = futures::future::try_join_all(services);

    let ctrl_c_signal = tokio::signal::ctrl_c();

    tokio::select! {
        _ = services => {
            bail!("Serives quit unexpectedly");
        }
        _ = ctrl_c_signal => {
            info!("Received ctrl+c signal");
            Ok(())
        }
    }
}
