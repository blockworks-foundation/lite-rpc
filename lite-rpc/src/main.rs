pub mod rpc_tester;

use std::time::Duration;

use anyhow::bail;
use clap::Parser;
use dotenv::dotenv;
use lite_rpc::postgres::Postgres;
use lite_rpc::service_spawner::ServiceSpawner;
use lite_rpc::{bridge::LiteBridge, cli::Args};
use lite_rpc::{DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE, GRPC_VERSION};

use solana_lite_rpc_cluster_endpoints::endpoint_stremers::EndpointStreaming;
use solana_lite_rpc_cluster_endpoints::grpc_subscription::create_grpc_subscription;
use solana_lite_rpc_cluster_endpoints::json_rpc_leaders_getter::JsonRpcLeaderGetter;
use solana_lite_rpc_cluster_endpoints::json_rpc_subscription::create_json_rpc_polling_subscription;
use solana_lite_rpc_core::block_information_store::{BlockInformation, BlockInformationStore};
use solana_lite_rpc_core::cluster_info::ClusterInfo;
use solana_lite_rpc_core::data_cache::{DataCache, SlotCache};
use solana_lite_rpc_core::notifications::NotificationSender;
use solana_lite_rpc_core::quic_connection_utils::QuicConnectionParameters;
use solana_lite_rpc_core::streams::BlockStream;
use solana_lite_rpc_core::structures::identity_stakes::IdentityStakes;
use solana_lite_rpc_core::structures::processed_block::ProcessedBlock;
use solana_lite_rpc_core::subscription_handler::SubscriptionHandler;
use solana_lite_rpc_core::tx_store::TxStore;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_lite_rpc_services::data_caching_service::DataCachingService;
use solana_lite_rpc_services::tpu_utils::tpu_connection_path::TpuConnectionPath;
use solana_lite_rpc_services::tpu_utils::tpu_service::{TpuService, TpuServiceConfig};
use solana_lite_rpc_services::transaction_replayer::TransactionReplayer;
use solana_lite_rpc_services::tx_sender::TxSender;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::env;
use std::sync::Arc;
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

async fn get_latest_block(
    mut block_stream: BlockStream,
    commitment_config: CommitmentConfig,
) -> ProcessedBlock {
    while let Ok(block) = block_stream.recv().await {
        if block.commitment_config == commitment_config {
            return block;
        }
    }
    panic!("Did  not recv blocks");
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
    let Args {
        rpc_addr,
        lite_rpc_ws_addr,
        lite_rpc_http_addr,
        fanout_size,
        enable_postgres,
        prometheus_addr,
        identity_keypair,
        maximum_retries_per_tx,
        transaction_retry_after_secs,
        quic_proxy_addr,
        use_grpc,
        grpc_addr,
        ..
    } = args;

    let validator_identity = Arc::new(get_identity_keypair(&identity_keypair).await);

    let retry_after = Duration::from_secs(transaction_retry_after_secs);

    let tpu_connection_path = configure_tpu_connection_path(quic_proxy_addr);

    // rpc client
    let rpc_client = Arc::new(RpcClient::new(rpc_addr.clone()));
    let (subscriptions, cluster_endpoint_tasks) = if use_grpc {
        create_grpc_subscription(rpc_client.clone(), grpc_addr, GRPC_VERSION.to_string())?
    } else {
        create_json_rpc_polling_subscription(rpc_client.clone())?
    };
    let EndpointStreaming {
        blocks_notifier,
        cluster_info_notifier,
        slot_notifier,
        vote_account_notifier,
    } = subscriptions;
    let finalized_block =
        get_latest_block(blocks_notifier.resubscribe(), CommitmentConfig::finalized()).await;

    let block_store = BlockInformationStore::new(BlockInformation::from_block(&finalized_block));
    let data_cache = DataCache {
        block_store,
        cluster_info: ClusterInfo::default(),
        identity_stakes: IdentityStakes::new(validator_identity.pubkey()),
        slot_cache: SlotCache::new(finalized_block.slot),
        tx_subs: SubscriptionHandler::default(),
        txs: TxStore::default(),
    };

    let lata_cache_service = DataCachingService {
        data_cache: data_cache.clone(),
        clean_duration: Duration::from_secs(120),
    };

    // to avoid laggin we resubscribe to block notification
    let data_caching_service = lata_cache_service.listen(
        blocks_notifier.resubscribe(),
        slot_notifier.resubscribe(),
        cluster_info_notifier,
        vote_account_notifier,
    );
    drop(blocks_notifier);

    let (notification_channel, postgres) = start_postgres(enable_postgres).await?;

    let tpu_config = TpuServiceConfig {
        fanout_slots: fanout_size,
        number_of_leaders_to_cache: 1024,
        clusterinfo_refresh_time: Duration::from_secs(60 * 60),
        leader_schedule_update_frequency: Duration::from_secs(10),
        maximum_transaction_in_queue: 20000,
        maximum_number_of_errors: 10,
        quic_connection_params: QuicConnectionParameters {
            connection_timeout: Duration::from_secs(1),
            connection_retry_count: 10,
            finalize_timeout: Duration::from_millis(200),
            max_number_of_connections: 10,
            unistream_timeout: Duration::from_millis(500),
            write_timeout: Duration::from_secs(1),
            number_of_transactions_per_unistream: 8,
        },
        tpu_connection_path,
    };

    let spawner = ServiceSpawner {
        prometheus_addr,
        data_cache: data_cache.clone(),
    };
    let leader_schedule = Arc::new(JsonRpcLeaderGetter::new(rpc_client.clone(), 1024, 128));

    let tpu_service: TpuService = TpuService::new(
        tpu_config,
        validator_identity,
        leader_schedule,
        data_cache.clone(),
    )
    .await?;
    let tx_sender = TxSender::new(data_cache.clone(), tpu_service.clone());
    let tx_replayer =
        TransactionReplayer::new(tpu_service.clone(), data_cache.txs.clone(), retry_after);
    let (transaction_service, tx_service_jh) = spawner.spawn_tx_service(
        tx_sender,
        tx_replayer,
        tpu_service,
        DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE,
        notification_channel.clone(),
        maximum_retries_per_tx,
        slot_notifier.resubscribe(),
    );
    drop(slot_notifier);

    let support_service = tokio::spawn(async move { spawner.spawn_support_services().await });

    let bridge_service = tokio::spawn(
        LiteBridge::new(rpc_client.clone(), data_cache.clone(), transaction_service)
            .start(lite_rpc_http_addr, lite_rpc_ws_addr),
    );
    tokio::select! {
        res = tx_service_jh => {
            anyhow::bail!("Tx Services {res:?}")
        }
        res = support_service => {
            anyhow::bail!("Support Services {res:?}")
        }
        res = bridge_service => {
            anyhow::bail!("Server {res:?}")
        }
        res = postgres => {
            anyhow::bail!("Postgres service {res:?}");
        }
        res = futures::future::select_all(data_caching_service) => {
            anyhow::bail!("Data caching service failed {res:?}")
        }
        res = futures::future::select_all(cluster_endpoint_tasks) => {
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
            bail!("")
        }
        _ = ctrl_c_signal => {
            log::info!("Received ctrl+c signal");

            Ok(())
        }
    }
}

fn configure_tpu_connection_path(quic_proxy_addr: Option<String>) -> TpuConnectionPath {
    match quic_proxy_addr {
        None => TpuConnectionPath::QuicDirectPath,
        Some(prox_address) => TpuConnectionPath::QuicForwardProxyPath {
            // e.g. "127.0.0.1:11111"
            forward_proxy_address: prox_address.parse().expect("Invalid proxy address"),
        },
    }
}
