pub mod rpc_tester;

use std::time::Duration;

use anyhow::{bail, Context};
use clap::Parser;
use dotenv::dotenv;
use lite_rpc::{bridge::LiteBridge, cli::Args};

use solana_lite_rpc_services::tpu_utils::tpu_connection_path::TpuConnectionPath;
use solana_sdk::signature::Keypair;
use std::env;
use std::sync::Arc;

use crate::rpc_tester::RpcTester;

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

pub async fn start_lite_rpc(args: Args) -> anyhow::Result<()> {
    let Args {
        rpc_addr,
        ws_addr,
        lite_rpc_ws_addr,
        lite_rpc_http_addr,
        fanout_size,
        enable_postgres,
        prometheus_addr,
        identity_keypair,
        maximum_retries_per_tx,
        transaction_retry_after_secs,
        experimental_quic_proxy_addr,
    } = args;

    let validator_identity = Arc::new(get_identity_keypair(&identity_keypair).await);

    let retry_after = Duration::from_secs(transaction_retry_after_secs);

    let tpu_connection_path = configure_tpu_connection_path(experimental_quic_proxy_addr);

    LiteBridge::new(
        rpc_addr,
        ws_addr,
        fanout_size,
        validator_identity,
        retry_after,
        maximum_retries_per_tx,
        tpu_connection_path,
    )
    .await
    .context("Error building LiteBridge")?
    .start_services(
        lite_rpc_http_addr,
        lite_rpc_ws_addr,
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

    let ctrl_c_signal = tokio::signal::ctrl_c();
    let rpc_tester = RpcTester::from(&args).start();

    let main = start_lite_rpc(args.clone());

    tokio::select! {
        err = rpc_tester => {
            // This should never happen
            unreachable!("{err:?}")
        }
        res = main => {
            // This should never happen
            log::error!("Services quit unexpectedly {res:?}");
            bail!("")
        }
        _ = ctrl_c_signal => {
            log::info!("Received ctrl+c signal");

            Ok(())
        }
    }
}

fn configure_tpu_connection_path(
    experimental_quic_proxy_addr: Option<String>,
) -> TpuConnectionPath {
    match experimental_quic_proxy_addr {
        None => TpuConnectionPath::QuicDirectPath,
        Some(prox_address) => TpuConnectionPath::QuicForwardProxyPath {
            // e.g. "127.0.0.1:11111"
            forward_proxy_address: prox_address.parse().unwrap(),
        },
    }
}
