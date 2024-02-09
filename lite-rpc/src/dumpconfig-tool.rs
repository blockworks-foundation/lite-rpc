use log::info;
use lite_rpc::cli::Config;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let config = Config::load().await?;

    // CAUTION: the output might contain credentials
    println!("Lite RPC effective config: \n{:?}", config);

    info!("Lite RPC effective config: {:?}", config);

    Ok(())
}