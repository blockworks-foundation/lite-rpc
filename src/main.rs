use std::time::Duration;

use anyhow::bail;
use clap::Parser;
use lite_rpc::{bridge::LiteBridge, cli::Args};
use log::info;
use solana_sdk::signature::Keypair;

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
        prometheus_addr,
        identity_keypair,
    } = Args::parse();

    let identity = if identity_keypair.is_empty() {
        Keypair::new()
    } else {
        let identity_file = tokio::fs::read_to_string(identity_keypair.as_str())
            .await
            .expect("Cannot find the identity file provided");
        let identity_bytes: Vec<u8> = serde_json::from_str(&identity_file)?;
        Keypair::from_bytes(identity_bytes.as_slice())?
    };

    let tx_batch_interval_ms = Duration::from_millis(tx_batch_interval_ms);
    let clean_interval_ms = Duration::from_millis(clean_interval_ms);

    let light_bridge = LiteBridge::new(rpc_addr, ws_addr, fanout_size, identity).await?;

    let services = light_bridge
        .start_services(
            lite_rpc_http_addr,
            lite_rpc_ws_addr,
            tx_batch_size,
            tx_batch_interval_ms,
            clean_interval_ms,
            enable_postgres,
            prometheus_addr,
        )
        .await?;

    let services = futures::future::try_join_all(services);

    let ctrl_c_signal = tokio::signal::ctrl_c();

    tokio::select! {
        _ = services => {
            bail!("Services quit unexpectedly");
        }
        _ = ctrl_c_signal => {
            info!("Received ctrl+c signal");
            Ok(())
        }
    }
}
