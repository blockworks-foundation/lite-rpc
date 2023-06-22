use std::time::Duration;

use anyhow::bail;
use clap::Parser;
use dotenv::dotenv;
use lite_rpc::{bridge::LiteBridge, cli::Args};
use log::{error, info};
use prometheus::{opts, register_int_counter, IntCounter};
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::signature::Keypair;
use std::env;

const RESTART_DURATION: Duration = Duration::from_secs(20);

async fn get_identity_keypair(identity_from_cli: &str) -> Keypair {
    if let Ok(identity_env_var) = env::var("IDENTITY") {
        if let Ok(identity_bytes) = serde_json::from_str::<Vec<u8>>(identity_env_var.as_str()) {
            Keypair::from_bytes(identity_bytes.as_slice()).unwrap()
        } else {
            // must be a file
            let identity_file = tokio::fs::read_to_string(identity_env_var.as_str())
                .await
                .expect("Cannot find the identity file provided");
            let identity_bytes: Vec<u8> = serde_json::from_str(&identity_file).unwrap();
            Keypair::from_bytes(identity_bytes.as_slice()).unwrap()
        }
    } else if identity_from_cli.is_empty() {
        Keypair::new()
    } else {
        let identity_file = tokio::fs::read_to_string(identity_from_cli)
            .await
            .expect("Cannot find the identity file provided");
        let identity_bytes: Vec<u8> = serde_json::from_str(&identity_file).unwrap();
        Keypair::from_bytes(identity_bytes.as_slice()).unwrap()
    }
}

lazy_static::lazy_static! {
    static ref RESTARTS: IntCounter =
    register_int_counter!(opts!("literpc_rpc_restarts", "Number of times lite rpc restarted")).unwrap();
}

pub async fn start_lite_rpc(args: Args) -> anyhow::Result<()> {
    let Args {
        rpc_addr,
        ws_addr,
        lite_rpc_ws_addr,
        lite_rpc_http_addr,
        clean_interval_ms,
        fanout_size,
        enable_postgres,
        prometheus_addr,
        identity_keypair,
        maximum_retries_per_tx,
        transaction_retry_after_secs,
    } = args;

    let identity = get_identity_keypair(&identity_keypair).await;

    let retry_after = Duration::from_secs(transaction_retry_after_secs);
    let clean_interval_ms = Duration::from_millis(clean_interval_ms);

    LiteBridge::new(
        rpc_addr,
        ws_addr,
        fanout_size,
        identity,
        retry_after,
        maximum_retries_per_tx,
    )
    .await?
    .start_services(
        lite_rpc_http_addr,
        lite_rpc_ws_addr,
        clean_interval_ms,
        enable_postgres,
        prometheus_addr,
    )
    .await
}

fn get_args() -> Args {
    let mut args = Args::parse();

    dotenv().ok();

    args.enable_postgres = args.enable_postgres
        || if let Ok(enable_postgres_env_var) = env::var("PG_ENABLED") {
            enable_postgres_env_var != "false"
        } else {
            false
        };

    args
}

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = get_args();

    let main: AnyhowJoinHandle = tokio::spawn(async move {
        loop {
            let Err(err) = start_lite_rpc(args.clone()).await else{
                bail!("LiteBridge services returned without error");
            };

            // log and restart
            log::error!("Services quit unexpectedly {err:?} restarting in {RESTART_DURATION:?}");
            tokio::time::sleep(RESTART_DURATION).await;

            // increment restart
            log::error!("Restarting services");
            RESTARTS.inc();
        }
    });

    let ctrl_c_signal = tokio::signal::ctrl_c();

    tokio::select! {
        res = main => {
            error!("LiteRpc exited with result {res:?}");
            bail!("LiteRpc quit with errors")
        }
        _ = ctrl_c_signal => {
            info!("Received ctrl+c signal");

            Ok(())
        }
    }
}
