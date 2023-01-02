use std::str::FromStr;

use anyhow::Context;
use clap::Parser;
use lite_rpc::{bridge::LiteBridge, cli::Args};
use reqwest::Url;
use simplelog::*;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    TermLogger::init(
        LevelFilter::Info,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )?;

    let Args {
        rpc_addr,
        ws_addr,
        lite_rpc_addr,
        batch_transactions,
    } = Args::parse();

    let light_bridge = LiteBridge::new(
        Url::from_str(&rpc_addr).unwrap(),
        &ws_addr,
        batch_transactions,
    )
    .await?;

    let services = light_bridge.start_services(lite_rpc_addr);
    let services = futures::future::try_join_all(services);

    let ctrl_c_signal = tokio::signal::ctrl_c();

    tokio::select! {
        services = services => {
            services.context("Some services exited unexpectedly")?;
        }
        _ = ctrl_c_signal => {}
    }

    Ok(())
}
