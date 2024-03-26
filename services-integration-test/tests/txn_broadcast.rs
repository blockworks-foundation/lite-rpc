use std::sync::Arc;

use bench::{create_memo_tx, create_rng, BenchmarkTransactionParams};
use dashmap::DashMap;
use itertools::Itertools;
use solana_lite_rpc_core::{keypair_loader::load_identity_keypair, stores::{block_information_store::{BlockInformation, BlockInformationStore}, cluster_info_store::ClusterInfo, data_cache::{DataCache, SlotCache}, subscription_store::SubscriptionStore, tx_store::TxStore}, structures::{account_filter::AccountFilters, epoch::EpochCache, identity_stakes::IdentityStakes, leaderschedule::CalculatedSchedule, produced_block::ProducedBlock}, types::BlockStream, AnyhowJoinHandle};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, hash::hash, signature::Keypair, signer::Signer};

use lite_rpc::{cli::Config, service_spawner::ServiceSpawner};
use lite_rpc::{DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE, MAX_NB_OF_CONNECTIONS_WITH_LEADERS};
use log::{debug, info};
use solana_lite_rpc_address_lookup_tables::address_lookup_table_store::AddressLookupTableStore;
use solana_lite_rpc_cluster_endpoints::{endpoint_stremers::EndpointStreaming, geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig}, grpc_subscription::create_grpc_subscription, json_rpc_leaders_getter::JsonRpcLeaderGetter};
use solana_lite_rpc_core::traits::address_lookup_table_interface::AddressLookupTableInterface;
use solana_lite_rpc_prioritization_fees::account_prio_service::AccountPrioService;
use solana_lite_rpc_services::{data_caching_service::DataCachingService, tpu_utils::tpu_connection_path::TpuConnectionPath, transaction_service::TransactionService};
use solana_lite_rpc_services::quic_connection_utils::QuicConnectionParameters;
use solana_lite_rpc_services::tpu_utils::tpu_service::{TpuService, TpuServiceConfig};
use solana_lite_rpc_services::transaction_replayer::TransactionReplayer;
use solana_lite_rpc_services::tx_sender::TxSender;

use solana_lite_rpc_prioritization_fees::start_block_priofees_task;
use std::time::Duration;
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::{io::AsyncReadExt, time::{timeout, Instant}};
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
    percentage_of_connection_limit_to_create_new: 10,
};


#[tokio::test]
/// TC 4
///- send txs on LiteRPC broadcast channel and consume them using the Solana quic-streamer
/// - see quic_proxy_tpu_integrationtest.rs (note: not only about proxy)
/// - run cargo test (maybe need to use release build)
/// - Goal: measure performance of LiteRPC internal channel/thread structure and the TPU_service performance
pub async fn txn_broadcast() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    info!("START BENCHMARK: txn_broadcast");

    // TODO: parse args without CLAP
    // let (transaction_service, tx_service_jh) = setup_txn_service().await?;
    setup_txn_service().await?;

    let mut rng = create_rng(None);
    let payer = Keypair::new();
    let mut blockhash = hash(&[1,2,3]);
    let params = BenchmarkTransactionParams { 
        tx_size: bench::tx_size::TxSize::Small, 
        cu_price_micro_lamports: 1 
    };

    let mut i = 0 ;
    while i < 3 {
        blockhash = hash(&blockhash.as_ref());
        let tx = create_memo_tx(&payer, blockhash, &mut rng, &params);
        // transaction_service.send_transaction(tx.message_data(), Some(1)).await?;
        i += 1;
    }

    Ok(())
}


// async fn setup_txn_service() -> anyhow::Result<(TransactionService, AnyhowJoinHandle)> {
    async fn setup_txn_service() -> anyhow::Result<()> {
    let config = Config::load().await?;
    let grpc_sources = config.get_grpc_sources();

    let Config {
        lite_rpc_ws_addr,
        rpc_addr,
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
        ..
    } = config;

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

    let rpc_client = Arc::new(RpcClient::new(rpc_addr.clone()));

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    info!("Creating geyser subscription...");
    let (subscriptions, cluster_endpoint_tasks) = 
        create_grpc_subscription(
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
        cluster_info_notifier,
        slot_notifier,
        vote_account_notifier,
        processed_account_stream,
    } = subscriptions;


    info!("Waiting for first finalized block...");
    let finalized_block =
        get_latest_block(blocks_notifier.resubscribe(), CommitmentConfig::finalized()).await;
    info!("Got finalized block: {:?}", finalized_block.slot);

    // let (epoch_data, _current_epoch_info) = EpochCache::bootstrap_epoch(&rpc_client).await?;

    // let block_information_store =
    //     BlockInformationStore::new(BlockInformation::from_block(&finalized_block));

    // let data_cache = DataCache {
    //     block_information_store,
    //     cluster_info: ClusterInfo::default(),
    //     identity_stakes: IdentityStakes::new(validator_identity.pubkey()),
    //     slot_cache: SlotCache::new(finalized_block.slot),
    //     tx_subs: SubscriptionStore::default(),
    //     txs: TxStore {
    //         store: Arc::new(DashMap::new()),
    //     },
    //     epoch_data,
    //     leader_schedule: Arc::new(RwLock::new(CalculatedSchedule::default())),
    // };

    // let tpu_config = TpuServiceConfig {
    //     fanout_slots: fanout_size,
    //     maximum_transaction_in_queue: 20000,
    //     quic_connection_params: QUIC_CONNECTION_PARAMS,
    //     tpu_connection_path,
    // };

    // let spawner = ServiceSpawner {
    //     prometheus_addr,
    //     data_cache: data_cache.clone(),
    // };

    // //init grpc leader schedule and vote account is configured.
    // let leader_schedule = Arc::new(JsonRpcLeaderGetter::new(rpc_client.clone(), 1024, 128));
    // let tpu_service: TpuService = TpuService::new(
    //     tpu_config,
    //     validator_identity,
    //     leader_schedule,
    //     data_cache.clone(),
    // )
    // .await?;

    // let tx_sender = TxSender::new(data_cache.clone(), tpu_service.clone());
    // let tx_replayer =
    //     TransactionReplayer::new(tpu_service.clone(), data_cache, retry_after);
    // let (transaction_service, tx_service_jh) = spawner.spawn_tx_service(
    //     tx_sender,
    //     tx_replayer,
    //     tpu_service,
    //     DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE,
    //     None,
    //     maximum_retries_per_tx,
    //     slot_notifier.resubscribe(),
    // );

    // Ok((transaction_service, tx_service_jh))
    Ok(())
}


// TODO: deduplicate
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