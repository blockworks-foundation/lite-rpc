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
use solana_lite_rpc_core::keypair_loader::load_identity_keypair;
use solana_lite_rpc_core::quic_connection_utils::QuicConnectionParameters;
use solana_lite_rpc_core::stores::{
    block_information_store::{BlockInformation, BlockInformationStore},
    cluster_info_store::ClusterInfo,
    data_cache::{DataCache, SlotCache},
    subscription_store::SubscriptionStore,
    tx_store::TxStore,
};
use solana_lite_rpc_core::structures::{
    identity_stakes::IdentityStakes, notifications::NotificationSender,
    produced_block::ProducedBlock,
};
use solana_lite_rpc_core::types::BlockStream;
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
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::rpc_tester::RpcTester;

async fn get_latest_block(
    mut block_stream: BlockStream,
    commitment_config: CommitmentConfig,
) -> ProducedBlock {
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

pub async fn start_lite_rpc(args: Args, rpc_client: Arc<RpcClient>) -> anyhow::Result<()> {
    let Args {
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

    let validator_identity = Arc::new(
        load_identity_keypair(&identity_keypair)
            .await
            .unwrap_or_else(Keypair::new),
    );

    let retry_after = Duration::from_secs(transaction_retry_after_secs);

    let tpu_connection_path = configure_tpu_connection_path(quic_proxy_addr);

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
        tx_subs: SubscriptionStore::default(),
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
        maximum_transaction_in_queue: 20000,
        quic_connection_params: QuicConnectionParameters {
            connection_timeout: Duration::from_secs(1),
            connection_retry_count: 10,
            finalize_timeout: Duration::from_millis(200),
            max_number_of_connections: 8,
            unistream_timeout: Duration::from_millis(500),
            write_timeout: Duration::from_secs(1),
            number_of_transactions_per_unistream: 1,
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
    let Args { rpc_addr, .. } = &args;
    // rpc client
    let rpc_client = Arc::new(RpcClient::new(rpc_addr.clone()));
    let rpc_tester = tokio::spawn(RpcTester::new(rpc_client.clone()).start());

    let main = start_lite_rpc(args.clone(), rpc_client);

    tokio::select! {
        err = rpc_tester => {
            log::error!("{err:?}");
            Ok(())
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
        Some(prox_address) => {
            let proxy_socket_addr = parse_host_port(prox_address.as_str()).unwrap();
            TpuConnectionPath::QuicForwardProxyPath {
                // e.g. "127.0.0.1:11111"
                forward_proxy_address: proxy_socket_addr,
            }
        }
    }
}

fn parse_host_port(host_port: &str) -> Result<SocketAddr, String> {
    let addrs: Vec<_> = host_port
        .to_socket_addrs()
        .map_err(|err| format!("Unable to resolve host {host_port}: {err}"))?
        .collect();
    if addrs.is_empty() {
        Err(format!("Unable to resolve host: {host_port}"))
    } else if addrs.len() > 1 {
        Err(format!("Multiple addresses resolved for host: {host_port}"))
    } else {
        Ok(addrs[0])
    }
}
