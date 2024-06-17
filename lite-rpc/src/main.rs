pub mod rpc_tester;

use crate::rpc_tester::RpcTester;
use anyhow::bail;
use dashmap::DashMap;
use lite_rpc::bridge::LiteBridge;
use lite_rpc::bridge_pubsub::LitePubSubBridge;
use lite_rpc::cli::Config;
use lite_rpc::postgres_logger::PostgresLogger;
use lite_rpc::service_spawner::ServiceSpawner;
use lite_rpc::start_server::start_servers;
use lite_rpc::DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE;
use log::info;
use solana_lite_rpc_accounts::account_service::AccountService;
use solana_lite_rpc_accounts::account_store_interface::AccountStorageInterface;
use solana_lite_rpc_accounts::inmemory_account_store::InmemoryAccountStore;
use solana_lite_rpc_accounts::simple_filter_store::SimpleFilterStore;
use solana_lite_rpc_accounts_on_demand::mutable_filter_store;
use solana_lite_rpc_accounts_on_demand::quic_plugin_accounts_on_demand::QuicPluginAccountsOnDemand;
use solana_lite_rpc_address_lookup_tables::address_lookup_table_store::AddressLookupTableStore;
use solana_lite_rpc_blockstore::history::History;
use solana_lite_rpc_cluster_endpoints::endpoint_stremers::EndpointStreaming;

use solana_lite_rpc_cluster_endpoints::geyser_grpc_connector::{
    GrpcConnectionTimeouts, GrpcSourceConfig,
};
use solana_lite_rpc_cluster_endpoints::grpc::grpc_inspect::{
    debugtask_blockstream_confirmation_sequence, debugtask_blockstream_slot_progression,
};
use solana_lite_rpc_cluster_endpoints::grpc::grpc_subscription::create_grpc_subscription;
use solana_lite_rpc_cluster_endpoints::json_rpc_leaders_getter::JsonRpcLeaderGetter;
use solana_lite_rpc_cluster_endpoints::json_rpc_subscription::create_json_rpc_polling_subscription;
use solana_lite_rpc_cluster_endpoints::quic::quic_subsciption::create_quic_endpoint;
use solana_lite_rpc_cluster_endpoints::rpc_polling::poll_blocks::NUM_PARALLEL_TASKS_DEFAULT;
use solana_lite_rpc_core::keypair_loader::load_identity_keypair;
use solana_lite_rpc_core::stores::{
    block_information_store::{BlockInformation, BlockInformationStore},
    cluster_info_store::ClusterInfo,
    data_cache::{DataCache, SlotCache},
    subscription_store::SubscriptionStore,
    tx_store::TxStore,
};
use solana_lite_rpc_core::structures::account_filter::AccountFilters;
use solana_lite_rpc_core::structures::leaderschedule::CalculatedSchedule;
use solana_lite_rpc_core::structures::{
    epoch::EpochCache, identity_stakes::IdentityStakes, notifications::NotificationSender,
};
use solana_lite_rpc_core::traits::address_lookup_table_interface::AddressLookupTableInterface;
use solana_lite_rpc_core::types::BlockStream;
use solana_lite_rpc_core::utils::wait_till_block_of_commitment_is_recieved;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_lite_rpc_prioritization_fees::account_prio_service::AccountPrioService;
use solana_lite_rpc_services::data_caching_service::DataCachingService;
use solana_lite_rpc_services::tpu_utils::tpu_connection_path::TpuConnectionPath;
use solana_lite_rpc_services::tpu_utils::tpu_service::{TpuService, TpuServiceConfig};
use solana_lite_rpc_services::transaction_replayer::TransactionReplayer;
use solana_lite_rpc_services::tx_sender::TxSender;

use lite_rpc::postgres_logger;
use solana_lite_rpc_prioritization_fees::start_block_priofees_task;
use solana_lite_rpc_util::obfuscate_rpcurl;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

// jemalloc seems to be better at keeping the memory footprint reasonable over
// longer periods of time
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub async fn start_postgres(
    config: Option<postgres_logger::PostgresSessionConfig>,
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

    let postgres_session_cache = postgres_logger::PostgresSessionCache::new(config).await?;
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
        enable_grpc_stream_inspection,
        enable_address_lookup_tables,
        address_lookup_tables_binary,
        account_filters,
        enable_accounts_on_demand_accounts_service,
        quic_connection_parameters,
        quic_geyser_plugin_config,
        ..
    } = args;

    let validator_identity = Arc::new(
        load_identity_keypair(identity_keypair)
            .await?
            .unwrap_or_else(Keypair::new),
    );

    let retry_after = Duration::from_secs(transaction_retry_after_secs);

    let tpu_connection_path = configure_tpu_connection_path(quic_proxy_addr);

    let account_filters = if let Some(account_filters) = account_filters {
        serde_json::from_str::<AccountFilters>(account_filters.as_str())
            .expect("Account filters should be valid")
    } else {
        vec![]
    };

    let enable_accounts_on_demand_accounts_service =
        enable_accounts_on_demand_accounts_service.unwrap_or_default();
    if enable_accounts_on_demand_accounts_service {
        log::info!("Accounts on demand service is enabled");
    } else {
        log::info!("Accounts on demand service is disabled");
    }

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(15),
        request_timeout: Duration::from_secs(15),
        subscribe_timeout: Duration::from_secs(15),
        receive_timeout: Duration::from_secs(15),
    };

    let ((subscriptions, cluster_endpoint_tasks), accounts_service) = if use_grpc {
        info!("Creating geyser subscription...");
        (
            create_grpc_subscription(
                rpc_client.clone(),
                grpc_sources
                    .iter()
                    .map(|s| {
                        GrpcSourceConfig::new(
                            s.addr.clone(),
                            s.x_token.clone(),
                            None,
                            timeouts.clone(),
                        )
                    })
                    .collect(),
                account_filters.clone(),
            )?,
            None,
        )
    } else if let Some(quic_geyser_plugin_config) = quic_geyser_plugin_config {
        info!("Using quic geyser subscription");
        let (client, endpoint, tasks) = create_quic_endpoint(
            rpc_client.clone(),
            quic_geyser_plugin_config.url,
            account_filters.clone(),
        )
        .await?;

        let accounts_service = if let Some(account_stream) = &endpoint.processed_account_stream {
            const MAX_CONNECTIONS_IN_PARALLEL: usize = 10;
            // Accounts notifications will be spurious when slots change
            // 256 seems very reasonable so that there are no account notification is missed and memory usage
            let (account_notification_sender, _) = tokio::sync::broadcast::channel(5000);

            let account_storage: Arc<dyn AccountStorageInterface> =
                if enable_accounts_on_demand_accounts_service {
                    // mutable filter store
                    let mutable_filters_store =
                        Arc::new(mutable_filter_store::MutableFilterStore::default());
                    mutable_filters_store
                        .add_account_filters(&account_filters)
                        .await;
                    Arc::new(QuicPluginAccountsOnDemand::new(
                        client,
                        rpc_client.clone(),
                        mutable_filters_store.clone(),
                        Arc::new(InmemoryAccountStore::new(mutable_filters_store)),
                    ))
                } else {
                    // no accounts on demand use const filter store
                    let mut simple_filter_store = SimpleFilterStore::default();
                    simple_filter_store.add_account_filters(&account_filters);
                    // lets use inmemory storage for now
                    Arc::new(InmemoryAccountStore::new(Arc::new(simple_filter_store)))
                };

            let account_service = AccountService::new(account_storage, account_notification_sender);

            account_service.process_account_stream(
                account_stream.resubscribe(),
                endpoint.blockinfo_notifier.resubscribe(),
            );

            account_service
                .populate_from_rpc(
                    rpc_client.url(),
                    &account_filters,
                    MAX_CONNECTIONS_IN_PARALLEL,
                )
                .await?;
            Some(account_service)
        } else {
            None
        };
        ((endpoint, tasks), accounts_service)
    } else {
        info!("Creating RPC poll subscription...");
        (
            create_json_rpc_polling_subscription(rpc_client.clone(), NUM_PARALLEL_TASKS_DEFAULT)?,
            None,
        )
    };
    let EndpointStreaming {
        // note: blocks_notifier will be dropped at some point
        blocks_notifier,
        blockinfo_notifier,
        cluster_info_notifier,
        slot_notifier,
        vote_account_notifier,
        ..
    } = subscriptions;

    if enable_grpc_stream_inspection {
        setup_grpc_stream_debugging(&blocks_notifier)
    } else {
        info!("Disabled grpc stream inspection");
    }

    info!("Waiting for first finalized block info...");
    let finalized_block_info = wait_till_block_of_commitment_is_recieved(
        blockinfo_notifier.resubscribe(),
        CommitmentConfig::finalized(),
    )
    .await;
    info!("Got finalized block info: {:?}", finalized_block_info.slot);

    let (epoch_data, _current_epoch_info) = EpochCache::bootstrap_epoch(&rpc_client).await?;

    let block_information_store =
        BlockInformationStore::new(BlockInformation::from_block_info(&finalized_block_info));

    let data_cache = DataCache {
        block_information_store,
        cluster_info: ClusterInfo::default(),
        identity_stakes: IdentityStakes::new(validator_identity.pubkey()),
        slot_cache: SlotCache::new(finalized_block_info.slot),
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
        blockinfo_notifier.resubscribe(),
        slot_notifier.resubscribe(),
        cluster_info_notifier,
        vote_account_notifier,
    );

    let (block_priofees_task, block_priofees_service) =
        start_block_priofees_task(blocks_notifier.resubscribe(), 100);

    let address_lookup_tables: Option<Arc<dyn AddressLookupTableInterface>> =
        if enable_address_lookup_tables.unwrap_or_default() {
            log::info!("ALTs enabled");
            let alts_store = AddressLookupTableStore::new(rpc_client.clone());
            if let Some(address_lookup_tables_binary) = address_lookup_tables_binary {
                match tokio::fs::File::open(address_lookup_tables_binary).await {
                    Ok(mut alts_file) => {
                        let mut buf = vec![];
                        alts_file.read_to_end(&mut buf).await.unwrap();
                        alts_store.load_binary(buf);

                        log::info!("{} ALTs loaded from binary file", alts_store.map.len());
                    }
                    Err(e) => {
                        log::error!("Error loading address lookup tables binary : {e:?}");
                        anyhow::bail!(e.to_string());
                    }
                }
            }
            Some(Arc::new(alts_store))
        } else {
            log::info!("ALTs disabled");
            None
        };

    let (account_priofees_task, account_priofees_service) =
        AccountPrioService::start_account_priofees_task(
            blocks_notifier.resubscribe(),
            100,
            address_lookup_tables,
        );

    let (notification_channel, postgres) = start_postgres(postgres).await?;

    let tpu_config = TpuServiceConfig {
        fanout_slots: fanout_size,
        maximum_transaction_in_queue: 20000,
        quic_connection_params: quic_connection_parameters.unwrap_or_default(),
        tpu_connection_path,
    };

    let spawner = ServiceSpawner {
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
        TransactionReplayer::new(tpu_service.clone(), data_cache.clone(), retry_after);
    let (transaction_service, tx_service_jh) = spawner.spawn_tx_service(
        tx_sender,
        tx_replayer,
        tpu_service,
        DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE,
        notification_channel.clone(),
        maximum_retries_per_tx,
        slot_notifier.resubscribe(),
    );

    let support_service =
        tokio::spawn(async move { spawner.spawn_support_services(prometheus_addr).await });

    let history = History::new();

    let rpc_service = LiteBridge::new(
        rpc_client.clone(),
        data_cache.clone(),
        transaction_service,
        history,
        block_priofees_service.clone(),
        account_priofees_service.clone(),
        accounts_service.clone(),
    );

    let pubsub_service = LitePubSubBridge::new(
        data_cache.clone(),
        block_priofees_service,
        account_priofees_service,
        blocks_notifier,
        accounts_service.clone(),
    );

    let bridge_service = tokio::spawn(start_servers(
        rpc_service,
        pubsub_service,
        lite_rpc_ws_addr,
        lite_rpc_http_addr,
        None,
    ));
    drop(slot_notifier);

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
        // allow it to fail
        // res = block_priofees_task => {
        //     anyhow::bail!("Prio Fees Service {res:?}")
        // }
        res = postgres => {
            anyhow::bail!("Postgres service {res:?}");
        }
        res = futures::future::select_all(data_caching_service) => {
            anyhow::bail!("Data caching service failed {res:?}")
        }
        res = futures::future::select_all(cluster_endpoint_tasks) => {
            anyhow::bail!("cluster endpoint failure {res:?}")
        }
        res = block_priofees_task => {
            anyhow::bail!("block prioritization fees task failed {res:?}")
        }
        res = account_priofees_task => {
            anyhow::bail!("account prioritization fees task failed {res:?}")
        }
    }
}

fn setup_grpc_stream_debugging(blocks_notifier: &BlockStream) {
    info!("Setting up grpc stream inspection");
    // note: check failes for commitment_config processed because sources might disagree on the blocks
    debugtask_blockstream_slot_progression(
        blocks_notifier.resubscribe(),
        CommitmentConfig::confirmed(),
    );
    debugtask_blockstream_slot_progression(
        blocks_notifier.resubscribe(),
        CommitmentConfig::finalized(),
    );
    debugtask_blockstream_confirmation_sequence(blocks_notifier.resubscribe());
}

#[tokio::main()]
pub async fn main() -> anyhow::Result<()> {
    setup_tracing_subscriber();

    let config = Config::load().await?;

    let ctrl_c_signal = tokio::signal::ctrl_c();
    let Config { rpc_addr, .. } = &config;
    // rpc client
    let rpc_client = Arc::new(RpcClient::new(rpc_addr.clone()));
    let rpc_tester = tokio::spawn(RpcTester::new(rpc_client.clone()).start(config.use_grpc));

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
            bail!("Service quit unexpectedly {res:?}");
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

fn setup_tracing_subscriber() {
    let enable_instrument_tracing = std::env::var("ENABLE_INSTRUMENT_TRACING")
        .unwrap_or("false".to_string())
        .parse::<bool>()
        .expect("flag must be true or false");

    if enable_instrument_tracing {
        tracing_subscriber::fmt::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            // not sure if "CLOSE" is exactly what we want
            // ex. "close time.busy=14.7ms time.idle=14.0µs"
            .with_span_events(FmtSpan::CLOSE)
            .init();
    } else {
        tracing_subscriber::fmt::init();
    }
}
