use dashmap::DashMap;
use solana_lite_rpc_core::{
    keypair_loader::load_identity_keypair,
    stores::{
        block_information_store::{BlockInformation, BlockInformationStore},
        cluster_info_store::ClusterInfo,
        data_cache::{DataCache, SlotCache},
        subscription_store::SubscriptionStore,
        tx_store::TxStore,
    },
    structures::{
        account_filter::AccountFilters, epoch::EpochCache, identity_stakes::IdentityStakes,
        leaderschedule::CalculatedSchedule, produced_block::ProducedBlock,
    },
    types::BlockStream, AnyhowJoinHandle,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig, signature::Keypair,
    signer::Signer,
};
use std::sync::Arc;

use lite_rpc::{cli::Config, service_spawner::ServiceSpawner, DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE};
use log::{debug, info, trace};
use solana_lite_rpc_cluster_endpoints::{
    endpoint_stremers::EndpointStreaming,
    geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig},
    grpc_subscription::create_grpc_subscription,
    json_rpc_leaders_getter::JsonRpcLeaderGetter,
};
use solana_lite_rpc_services::{tpu_utils::tpu_connection_path::TpuConnectionPath, transaction_replayer::TransactionReplayer, transaction_service::TransactionService};
use solana_lite_rpc_services::tpu_utils::tpu_service::{TpuService, TpuServiceConfig};
use solana_lite_rpc_services::{
    quic_connection_utils::QuicConnectionParameters, tx_sender::TxSender,
};

use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;
use tokio::time::{timeout, Instant};
use tokio::sync::RwLock;

// TODO: refactor
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
                debug!(
                    "waiting for latest block ({}) ... {:.02}ms",
                    commitment_config.commitment,
                    started.elapsed().as_secs_f32() * 1000.0
                );
            }
            Ok(Err(_error)) => {
                panic!("Did not recv blocks");
            }
        }
    }
}

const QUIC_CONNECTION_PARAMS: QuicConnectionParameters = QuicConnectionParameters {
    connection_timeout: Duration::from_secs(2),
    connection_retry_count: 10,
    finalize_timeout: Duration::from_secs(2),
    max_number_of_connections: 8,
    unistream_timeout: Duration::from_secs(2),
    write_timeout: Duration::from_secs(2),
    number_of_transactions_per_unistream: 10,
    unistreams_to_create_new_connection_in_percentage: 10,
};

pub async fn setup_tx_service() -> anyhow::Result<(TransactionService, DataCache, AnyhowJoinHandle)> {
    let config = Config::load().await?;
    let grpc_sources = config.get_grpc_sources();

    let Config {
        rpc_addr,
        fanout_size,
        identity_keypair,
        transaction_retry_after_secs,
        quic_proxy_addr,
        prometheus_addr,
        maximum_retries_per_tx,
        account_filters,
        ..
    } = config;

    let validator_identity = Arc::new(
        load_identity_keypair(identity_keypair)
            .await?
            .unwrap_or_else(Keypair::new),
    );

    let retry_after = Duration::from_secs(transaction_retry_after_secs);
    let tpu_connection_path = configure_tpu_connection_path(quic_proxy_addr);
    trace!("tpu_connection_path: {}", tpu_connection_path);

    let account_filters = if let Some(account_filters) = account_filters {
        serde_json::from_str::<AccountFilters>(account_filters.as_str())
            .expect("Account filters should be valid")
    } else {
        vec![]
    };

    let rpc_client = Arc::new(RpcClient::new(rpc_addr.clone()));

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    info!("Creating geyser subscription...");
    let (subscriptions, _cluster_endpoint_tasks) = create_grpc_subscription(
        rpc_client.clone(),
        grpc_sources
            .iter()
            .map(|s| {
                GrpcSourceConfig::new(s.addr.clone(), s.x_token.clone(), None, timeouts.clone())
            })
            .collect(),
        account_filters.clone(),
    )?;

    let EndpointStreaming {
        // note: blocks_notifier will be dropped at some point
        blocks_notifier,
        slot_notifier,
        ..
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

    let tpu_config = TpuServiceConfig {
        fanout_slots: fanout_size,
        maximum_transaction_in_queue: 20000,
        quic_connection_params: QUIC_CONNECTION_PARAMS,
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
    let tx_replayer = TransactionReplayer::new(tpu_service.clone(), data_cache.clone(), retry_after);

    trace!("spawning tx_service");
    let (transaction_service, tx_service_jh) = spawner.spawn_tx_service(
        tx_sender,
        tx_replayer,
        tpu_service,
        DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE,
        None,
        maximum_retries_per_tx,
        slot_notifier.resubscribe(),
    );
    trace!("tx_service spawned successfully");

    Ok((transaction_service, data_cache, tx_service_jh))
}

// TODO: deduplicate
pub fn configure_tpu_connection_path(quic_proxy_addr: Option<String>) -> TpuConnectionPath {
    match quic_proxy_addr {
        None => {
            TpuConnectionPath::QuicDirectPath
        },
        Some(prox_address) => {
            let proxy_socket_addr = parse_host_port(prox_address.as_str()).unwrap();
            TpuConnectionPath::QuicForwardProxyPath {
                // e.g. "127.0.0.1:11111" or "localhost:11111"
                forward_proxy_address: proxy_socket_addr,
            }
        }
    }
}

pub fn parse_host_port(host_port: &str) -> Result<SocketAddr, String> {
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
