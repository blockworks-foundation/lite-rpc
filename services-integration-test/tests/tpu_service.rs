use anyhow::bail;
use bench::{create_memo_tx, create_rng, BenchmarkTransactionParams};
use dashmap::DashMap;
use solana_lite_rpc_core::{
    keypair_loader::load_identity_keypair,
    solana_utils::SerializableTransaction,
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
        transaction_sent_info::SentTransactionInfo,
    },
    types::{BlockStream, SlotStream},
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig, hash::hash, nonce::state::Data, signature::Keypair,
    signer::Signer, transaction::VersionedTransaction,
};
use std::{process::abort, sync::Arc};

use lite_rpc::cli::Config;
use log::{debug, info, trace, warn};
use solana_lite_rpc_cluster_endpoints::{
    endpoint_stremers::EndpointStreaming,
    geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig},
    grpc_subscription::create_grpc_subscription,
    json_rpc_leaders_getter::JsonRpcLeaderGetter,
};
use solana_lite_rpc_services::tpu_utils::tpu_connection_path::TpuConnectionPath;
use solana_lite_rpc_services::tpu_utils::tpu_service::{TpuService, TpuServiceConfig};
use solana_lite_rpc_services::{
    quic_connection_utils::QuicConnectionParameters, tx_sender::TxSender,
};

use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;
use tokio::time::{timeout, Instant};
use tokio::{sync::RwLock, task::JoinHandle};

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

#[tokio::test]
/// - Goal: measure TPU_service performance
pub async fn tpu_service() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    info!("START BENCHMARK: tpu_service");

    let (tpu_service, data_cache, slot_stream) = setup_tpu_service().await?;

    // let mut slot_receiver = slot_stream.subscribe();
    let tpu_jh = tpu_service.start(slot_stream.resubscribe());

    info!("before unwrap");
    let _ = tpu_jh.await.unwrap();
    info!("after unwrap");

    // let mut receiver = tpu_service.broadcast_sender.subscribe();

    // let handle = tokio::spawn(async move {
    //     loop {
    //         tokio::select! {
    //             r = receiver.recv() => {
    //                 match r {
    //                     Ok(_) => {} //info!("{:?}", msg)
    //                 Err(e) => {
    //                     warn!("recv error: {:?}", e);
    //                     abort();
    //                     }
    //                 }
    //             }
    //             // s = slot_receiver.recv() => {
    //             //     match s {
    //             //         Ok(_) => {debug!("slot")},
    //             //         Err(e) => warn!("recv error: {:?}", e),
    //             //     }
    //             // }
    //         }
    //     }
    // });

    // info!("tpu serv inited");

    // let mut rng = create_rng(None);
    // let payer = Keypair::new();
    // let mut blockhash = hash(&[1, 2, 3]);

    // let prio_fee = 1u64;
    // let params = BenchmarkTransactionParams {
    //     tx_size: bench::tx_size::TxSize::Small,
    //     cu_price_micro_lamports: prio_fee,
    // };

    // let mut i = 0;
    // while i < 3 {
    //     blockhash = hash(&blockhash.as_ref());
    //     let tx = create_memo_tx(&payer, blockhash, &mut rng, &params);

    //     let sent_tx = SentTransactionInfo {
    //         signature: *tx.get_signature(),
    //         slot: data_cache.slot_cache.get_current_slot(),
    //         transaction: bincode::serialize::<VersionedTransaction>(&tx)
    //             .expect("Could not serialize VersionedTransaction"),
    //         last_valid_block_height: data_cache.block_information_store.get_last_blockheight(),
    //         prioritization_fee: prio_fee,
    //     };

    //     info!("Sending txn: {:?}", i);
    //     tpu_service.send_transaction(&sent_tx)?;
    //     info!("Txn sent success");
    //     i += 1;
    // }

    // handle.await?;

    Ok(())
}

async fn setup_tpu_service() -> anyhow::Result<(TpuService, DataCache, SlotStream)> {
    let config = Config::load().await?;
    let grpc_sources = config.get_grpc_sources();

    let Config {
        rpc_addr,
        fanout_size,
        identity_keypair,
        quic_proxy_addr,
        account_filters,
        ..
    } = config;

    let validator_identity = Arc::new(
        load_identity_keypair(identity_keypair)
            .await?
            .unwrap_or_else(Keypair::new),
    );

    let tpu_connection_path = configure_tpu_connection_path(quic_proxy_addr);

    let account_filters = if let Some(account_filters) = account_filters {
        serde_json::from_str::<AccountFilters>(account_filters.as_str())
            .expect("Account filters should be valid")
    } else {
        vec![]
    };

    let rpc_client = Arc::new(RpcClient::new(rpc_addr.clone()));
    info!("RPC: {:?}", rpc_addr.clone());

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };

    info!("Creating geyser subscription...");
    let (subscriptions, _) = create_grpc_subscription(
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

    //init grpc leader schedule and vote account is configured.
    let leader_schedule = Arc::new(JsonRpcLeaderGetter::new(rpc_client.clone(), 1024, 128));

    trace!("spawning TpuService");
    let tpu_service: TpuService = TpuService::new(
        tpu_config,
        validator_identity,
        leader_schedule,
        data_cache.clone(),
    )
    .await?;
    trace!("TpuService created successfully");

    Ok((tpu_service, data_cache, slot_notifier))
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
