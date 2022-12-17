use std::str::FromStr;

use clap::Parser;
use lite_rpc::bridge::LightBridge;
use lite_rpc::cli::Args;
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
    } = Args::parse();

    let light_bridge = LightBridge::new(Url::from_str(&rpc_addr).unwrap(), &ws_addr).await?;

    let services = light_bridge.start_services(lite_rpc_addr);
    let services = futures::future::join_all(services);

    let ctrl_c_signal = tokio::signal::ctrl_c();

    tokio::select! {
        services = services => {
            for res in services {
                res??;
            }
            anyhow::bail!("Some services exited unexpectedly")
        }
        _ = ctrl_c_signal => {
            Ok(())
        }
    }
}
