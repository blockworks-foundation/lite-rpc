mod rpc_tester;

use std::{sync::Arc, time::Duration};

use clap::Parser;
use dotenv::dotenv;
use lite_rpc::postgres::Postgres;
use lite_rpc::{bridge::LiteBridge, cli::Args};

use solana_lite_rpc_core::ledger::Ledger;
use solana_lite_rpc_core::notifications::NotificationSender;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_lite_rpc_services::{spawner::Spawner, tx_service::TxServiceConfig};
use solana_sdk::signature::Keypair;
use std::env;
use tokio::sync::mpsc;

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

pub async fn start_postgres(
    enable: bool,
) -> anyhow::Result<(Option<NotificationSender>, AnyhowJoinHandle)> {
    if !enable {
        return Ok((
            None,
            tokio::spawn(async {
                std::future::pending::<()>().await;
                unreachable!()
            }),
        ));
    }

    let (postgres_send, postgres_recv) = mpsc::unbounded_channel();

    let postgres = Postgres::new().await?.start(postgres_recv);

    Ok((Some(postgres_send), postgres))
}

pub async fn start_lite_rpc(args: Args) -> anyhow::Result<()> {
    // get all configs
    let Args {
        rpc_addr,
        use_grpc,
        grpc_addr,
        lite_rpc_ws_addr,
        lite_rpc_http_addr,
        fanout_size,
        enable_postgres,
        prometheus_addr,
        identity_keypair,
        maximum_retries_per_tx,
        transaction_retry_after_secs,
    } = args;

    let identity = Arc::new(get_identity_keypair(&identity_keypair).await);

    let retry_after = Duration::from_secs(transaction_retry_after_secs);

    let addr = if use_grpc {
        grpc_addr
    } else {
        rpc_addr.clone()
    };

    // tx service config
    let tx_service_config = TxServiceConfig {
        identity,
        fanout_slots: fanout_size,
        max_nb_txs_in_queue: 40, // TODO: fix this
        max_retries: maximum_retries_per_tx,
        retry_after,
    };

    // postgres
    let (notification_channel, postgres) = start_postgres(enable_postgres).await?;

    // setup ledger
    let ledger = Ledger::default();

    // spawner
    let spawner = Spawner {
        prometheus_addr,
        grpc: use_grpc,
        addr,
        tx_service_config,
        rpc_addr,
        ledger: Ledger::default(),
        notification_channel,
    };
    // start services
    let leger_service = spawner.spawn_ledger_service();
    let (tx_sender, tx_services) = spawner.spawn_tx_service().await?;
    let support_service = spawner.spawn_support_services();

    // lite bridge
    let lite_bridge = LiteBridge { ledger, tx_sender };

    let bridge_serivce = lite_bridge.start_services(lite_rpc_ws_addr, lite_rpc_http_addr);

    tokio::select! {
        leger_service = leger_service => {
            anyhow::bail!("{leger_service:?}")
        }
        tx_service = tx_services => {
            anyhow::bail!("{tx_service:?}")
        }
        support_service = support_service => {
            anyhow::bail!("{support_service:?}")
        }
        bridge_serivce = bridge_serivce => {
            anyhow::bail!("{bridge_serivce:?}")
        }
        res = postgres => {
            anyhow::bail!("Postgres service {res:?}");
        }
    }
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
            anyhow::bail!("")
        }
        _ = ctrl_c_signal => {
            log::info!("Received ctrl+c signal");

            Ok(())
        }
    }
}
