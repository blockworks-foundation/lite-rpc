pub mod rpc_tester;

use crate::rpc_tester::RpcTester;
use anyhow::bail;
use dashmap::DashMap;
use lite_rpc::bridge::LiteBridge;
use lite_rpc::cli::Config;
use lite_rpc::postgres_logger::PostgresLogger;
use lite_rpc::service_spawner::ServiceSpawner;
use lite_rpc::DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE;
use log::{debug, info};
use solana_lite_rpc_cluster_endpoints::endpoint_stremers::EndpointStreaming;
use solana_lite_rpc_cluster_endpoints::grpc_subscription::create_grpc_subscription;
use solana_lite_rpc_cluster_endpoints::grpc_subscription_autoreconnect::{
    GrpcConnectionTimeouts, GrpcSourceConfig,
};
use solana_lite_rpc_cluster_endpoints::json_rpc_leaders_getter::JsonRpcLeaderGetter;
use solana_lite_rpc_cluster_endpoints::json_rpc_subscription::create_json_rpc_polling_subscription;
use solana_lite_rpc_core::keypair_loader::load_identity_keypair;
use solana_lite_rpc_core::stores::{
    block_information_store::{BlockInformation, BlockInformationStore},
    cluster_info_store::ClusterInfo,
    data_cache::{DataCache, SlotCache},
    subscription_store::SubscriptionStore,
    tx_store::TxStore,
};
use solana_lite_rpc_core::structures::leaderschedule::CalculatedSchedule;
use solana_lite_rpc_core::structures::{
    epoch::EpochCache, identity_stakes::IdentityStakes, notifications::NotificationSender,
    produced_block::ProducedBlock,
};
use solana_lite_rpc_core::types::BlockStream;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_lite_rpc_history::block_stores::inmemory_block_store::InmemoryBlockStore;
use solana_lite_rpc_history::history::History;
use solana_lite_rpc_history::postgres::postgres_config::PostgresSessionConfig;
use solana_lite_rpc_history::postgres::postgres_session::PostgresSessionCache;
use solana_lite_rpc_services::data_caching_service::DataCachingService;
use solana_lite_rpc_services::quic_connection_utils::QuicConnectionParameters;
use solana_lite_rpc_services::tpu_utils::tpu_connection_path::TpuConnectionPath;
use solana_lite_rpc_services::tpu_utils::tpu_service::{TpuService, TpuServiceConfig};
use solana_lite_rpc_services::transaction_replayer::TransactionReplayer;
use solana_lite_rpc_services::tx_sender::TxSender;

use solana_lite_rpc_block_priofees::block_priofees;

use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time::{Instant, timeout};

async fn get_latest_block(
    mut block_stream: BlockStream,
    commitment_config: CommitmentConfig,
) -> ProducedBlock {
    let started = Instant::now();
    loop {
        match timeout(Duration::from_millis(500), block_stream.recv()).await {
            Ok(Ok(block)) => {
                if block.commitment_config == commitment_config {
                    return block;
                }
            }
            Err(_elapsed) => {
                debug!("waiting for latest block ({}) ... {:.02}ms",
                    commitment_config.commitment, started.elapsed().as_secs_f32() * 1000.0);
            }
            Ok(Err(_error)) => {
                panic!("Did not recv blocks");
            }
        }
    }
}

pub async fn start_postgres(
    config: Option<PostgresSessionConfig>,
) -> anyhow::Result<(Option<NotificationSender>, AnyhowJoinHandle)> {
    let Some(config) = config else {
        return Ok((
            None,
            tokio::spawn(async {
                std::future::pending::<()>().await;
                unreachable!()
            }),
        ));
    };

    let (postgres_send, postgres_recv) = mpsc::unbounded_channel();

    let postgres_session_cache = PostgresSessionCache::new(config).await?;
    let postgres = PostgresLogger::start(postgres_session_cache, postgres_recv);

    Ok((Some(postgres_send), postgres))
}

pub async fn start_lite_rpc(args: Config, rpc_client: Arc<RpcClient>) -> anyhow::Result<()> {
    let grpc_sources = args.get_grpc_sources();
    log::info!("grpc_sources:{grpc_sources:?}");
    let Config {
        lite_rpc_ws_addr,
        lite_rpc_http_addr,
        fanout_size,
        postgres,
        prometheus_addr,
        identity_keypair,
        maximum_retries_per_tx,
        transaction_retry_after_secs,
        quic_proxy_addr,
        use_grpc,
        ..
    } = args;

    let validator_identity = Arc::new(
        load_identity_keypair(identity_keypair)
            .await?
            .unwrap_or_else(Keypair::new),
    );

    let retry_after = Duration::from_secs(transaction_retry_after_secs);

    let tpu_connection_path = configure_tpu_connection_path(quic_proxy_addr);

    let (subscriptions, cluster_endpoint_tasks) = if use_grpc {
        info!("Creating geyser subscription...");

        let timeouts = GrpcConnectionTimeouts {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            subscribe_timeout: Duration::from_secs(5),
        };

        create_grpc_subscription(
            rpc_client.clone(),
            grpc_sources
                .iter()
                .map(|s| {
                    GrpcSourceConfig::new(s.addr.clone(), s.x_token.clone(), None, timeouts.clone())
                })
                .collect(),
        )?

        // create_grpc_subscription(
        //     rpc_client.clone(),
        //     grpc_addr.clone(),
        //     GRPC_VERSION.to_string(),
        // )?
    } else {
        info!("Creating RPC poll subscription...");
        create_json_rpc_polling_subscription(rpc_client.clone())?
    };
    let EndpointStreaming {
        // note: blocks_notifier will be dropped at some point
        blocks_notifier,
        cluster_info_notifier,
        slot_notifier,
        vote_account_notifier,
    } = subscriptions;

    info!("Waiting for first finalized block...");
    let finalized_block =
        get_latest_block(blocks_notifier.resubscribe(), CommitmentConfig::finalized()).await;
    info!("Got finalized block: {:?}", finalized_block.slot);

    let (epoch_data, _current_epoch_info) = EpochCache::bootstrap_epoch(&rpc_client).await?;

    let block_information_store =
        BlockInformationStore::new(BlockInformation::from_block(&finalized_block));

    let data_cache = DataCache {
        block_information_store,
        cluster_info: ClusterInfo::default(),
        identity_stakes: IdentityStakes::new(validator_identity.pubkey()),
        slot_cache: SlotCache::new(finalized_block.slot),
        tx_subs: SubscriptionStore::default(),
        txs: TxStore {
            store: Arc::new(DashMap::new()),
        },
        epoch_data,
        leader_schedule: Arc::new(RwLock::new(CalculatedSchedule::default())),
    };

    let data_cache_service = DataCachingService {
        data_cache: data_cache.clone(),
        clean_duration: Duration::from_secs(120),
    };

    // to avoid laggin we resubscribe to block notification
    let data_caching_service = data_cache_service.listen(
        blocks_notifier.resubscribe(),
        slot_notifier.resubscribe(),
        cluster_info_notifier,
        vote_account_notifier,
    );

    let (block_prio_fees_task, block_prio_fees_service) =
        block_priofees::start_priofees_task(
            data_cache.clone(),
            blocks_notifier.resubscribe()).await;

    drop(blocks_notifier);

    let (notification_channel, postgres) = start_postgres(postgres).await?;

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
    //init grpc leader schedule and vote account is configured.
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

    let history = History {
        block_storage: Arc::new(InmemoryBlockStore::new(1024)),
    };

    let bridge_service = tokio::spawn(
        LiteBridge::new(
            rpc_client.clone(),
            data_cache.clone(),
            transaction_service,
            history,
            block_prio_fees_service,
        )
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
        res = block_prio_fees_task => {
            anyhow::bail!("Prio Fees Service {res:?}")
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

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = Config::load().await?;

    let ctrl_c_signal = tokio::signal::ctrl_c();
    let Config { rpc_addr, .. } = &config;
    // rpc client
    let rpc_client = Arc::new(RpcClient::new(rpc_addr.clone()));
    let rpc_tester = tokio::spawn(RpcTester::new(rpc_client.clone()).start());

    info!("Use RPC address: {}", obfuscate_rpcurl(rpc_addr));

    let main = start_lite_rpc(config, rpc_client);

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
                // e.g. "127.0.0.1:11111" or "localhost:11111"
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

// http://mango.rpcpool.com/c232ab232ba2323
fn obfuscate_rpcurl(rpc_addr: &str) -> String {
    if rpc_addr.contains("rpcpool.com") {
        return rpc_addr.replacen(char::is_numeric, "X", 99);
    }
    rpc_addr.to_string()
}
