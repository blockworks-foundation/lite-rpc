mod rpc_tester;

use std::{sync::Arc, time::Duration};

use anyhow::{bail, Context};
use clap::Parser;
use dotenv::dotenv;
use lite_rpc::{bridge::LiteBridge, cli::Args, postgres::Postgres};

use solana_lite_rpc_cluster_endpoints::{
    json_rpc_leaders_getter::JsonRpcLeaderGetter,
    json_rpc_subscription::create_json_rpc_polling_subscription,
};
use solana_lite_rpc_core::{
    cluster_info::ClusterInfo, data_cache::DataCache, leader_schedule::LeaderSchedule,
    notifications::NotificationSender, AnyhowJoinHandle,
};
use solana_lite_rpc_services::{
    data_caching_service::DataCachingService, spawner::Spawner, tx_service::TxServiceConfig,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
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

    // rpc client
    let rpc_client = Arc::new(RpcClient::new(rpc_addr.clone()));
    if let Err(e) = rpc_client.get_version().await {
        bail!("Error connecting to rpc client {e:?}");
    }

    // setup ledger
    let data_cache = DataCache::default();

    // spawner
    let spawner = Spawner {
        prometheus_addr,
        addr,
        tx_service_config,
        rpc_addr,
        data_cache: data_cache.clone(),
        notification_channel,
    };
    let (subscriptions, cluster_endpoint_tasks) =
        create_json_rpc_polling_subscription(rpc_client.clone())?;

    let cluster_info = ClusterInfo::default();
    let cluster_info_jh: AnyhowJoinHandle = {
        let cluster_info: ClusterInfo = cluster_info.clone();
        let cluster_info_subsription = subscriptions.cluster_info_notifier.resubscribe();
        tokio::spawn(async move {
            let mut cluster_info_subsription = cluster_info_subsription;
            loop {
                cluster_info
                    .load_cluster_info(&mut cluster_info_subsription)
                    .await
                    .context("Failed to load cluster info")?;
            }
        })
    };

    // implement leader getter
    let leader_getter = Arc::new(JsonRpcLeaderGetter {
        rpc_client: rpc_client.clone(),
    });

    // leader schedule
    let leader_schedule = Arc::new(LeaderSchedule::new(1024, leader_getter, cluster_info));
    let leader_schedule_update_task: AnyhowJoinHandle = {
        let leader_schedule = leader_schedule.clone();
        let slot_subscription = subscriptions.slot_notifier.resubscribe();
        tokio::spawn(async move {
            let mut slot_subscription = slot_subscription;
            loop {
                let slot_notification = slot_subscription.recv().await;
                match slot_notification {
                    Ok(slot_notification) => {
                        leader_schedule
                            .update_leader_schedule(
                                slot_notification.processed_slot,
                                slot_notification.estimated_processed_slot,
                            )
                            .await?;
                    }
                    Err(e) => {
                        bail!("Error updating leader schdule {e:?}");
                    }
                }
            }
        })
    };

    // data caching service
    let data_caching_service = DataCachingService {
        data_cache: data_cache.clone(),
    };
    let data_caching_service_jhs = data_caching_service.listen(
        subscriptions.blocks_notifier.resubscribe(),
        subscriptions.slot_notifier.resubscribe(),
    );

    // start services
    let vote_account_notifier = subscriptions.vote_account_notifier.resubscribe();
    let (tx_sender, tx_services) = spawner
        .spawn_tx_service(leader_schedule, vote_account_notifier)
        .await?;
    let support_service = spawner.spawn_support_services();

    // lite bridge
    let bridge_serivce = LiteBridge {
        data_cache,
        tx_sender,
    }
    .start(lite_rpc_http_addr, lite_rpc_ws_addr);

    let cluster_endpoint_tasks = futures::future::select_all(cluster_endpoint_tasks);
    drop(subscriptions);
    tokio::select! {
        res = tx_services => {
            anyhow::bail!("Tx Services {res:?}")
        }
        res = support_service => {
            anyhow::bail!("Support Services {res:?}")
        }
        res = bridge_serivce => {
            anyhow::bail!("Server {res:?}")
        }
        res = postgres => {
            anyhow::bail!("Postgres service {res:?}");
        }
        res = cluster_info_jh => {
            anyhow::bail!("Contact info update service failed {res:?}")
        }
        res = leader_schedule_update_task => {
            anyhow::bail!("Leader schedule update task failed {res:?}")
        }
        res = futures::future::select_all(data_caching_service_jhs) => {
            anyhow::bail!("Data caching service failed {res:?}")
        }
        res = cluster_endpoint_tasks => {
            anyhow::bail!("cluster endpoint failure {res:?}")
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
