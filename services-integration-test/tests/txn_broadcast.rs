use std::sync::Arc;

use bench::{create_memo_tx, create_rng, BenchmarkTransactionParams};
use dashmap::DashMap;
use itertools::Itertools;
use serde::Serialize;
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
    types::BlockStream,
    AnyhowJoinHandle,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig, hash::hash, signature::Keypair, signer::Signer,
    transaction::VersionedTransaction,
};

use lite_rpc::{cli::Config, service_spawner::ServiceSpawner};
use lite_rpc::{DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE, MAX_NB_OF_CONNECTIONS_WITH_LEADERS};
use log::{debug, info, trace};
use solana_lite_rpc_address_lookup_tables::address_lookup_table_store::AddressLookupTableStore;
use solana_lite_rpc_cluster_endpoints::{
    endpoint_stremers::EndpointStreaming,
    geyser_grpc_connector::{GrpcConnectionTimeouts, GrpcSourceConfig},
    grpc_subscription::create_grpc_subscription,
    json_rpc_leaders_getter::JsonRpcLeaderGetter,
};
use solana_lite_rpc_core::traits::address_lookup_table_interface::AddressLookupTableInterface;
use solana_lite_rpc_prioritization_fees::account_prio_service::AccountPrioService;
use solana_lite_rpc_services::quic_connection_utils::QuicConnectionParameters;
use solana_lite_rpc_services::tpu_utils::tpu_service::{TpuService, TpuServiceConfig};
use solana_lite_rpc_services::transaction_replayer::TransactionReplayer;
use solana_lite_rpc_services::tx_sender::TxSender;
use solana_lite_rpc_services::{
    data_caching_service::DataCachingService, tpu_utils::tpu_connection_path::TpuConnectionPath,
    transaction_service::TransactionService,
};

use solana_lite_rpc_prioritization_fees::start_block_priofees_task;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::{
    io::AsyncReadExt,
    time::{timeout, Instant},
};

use crate::setup::setup_tx_service;

mod setup;

#[tokio::test]
/// TC 4
///- send txs on LiteRPC broadcast channel and consume them using the Solana quic-streamer
/// - see quic_proxy_tpu_integrationtest.rs (note: not only about proxy)
/// - run cargo test (maybe need to use release build)
/// - Goal: measure performance of LiteRPC internal channel/thread structure and the TPU_service performance
pub async fn txn_broadcast() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    info!("START BENCHMARK: txn_broadcast");

    debug!("spawning tx_service");
    let (transaction_service, tx_jh) = setup_tx_service().await?;
    debug!("tx_service spawned successfully");

    let mut rng = create_rng(None);
    let payer = Keypair::new();
    let mut blockhash = hash(&[1, 2, 3]);
    let params = BenchmarkTransactionParams {
        tx_size: bench::tx_size::TxSize::Small,
        cu_price_micro_lamports: 1,
    };

    let mut i = 0;
    while i < 3 {
        blockhash = hash(&blockhash.as_ref());
        let tx = create_memo_tx(&payer, blockhash, &mut rng, &params);

        info!("Sending txn: {:?}", i);
        transaction_service
            .send_transaction(
                bincode::serialize::<VersionedTransaction>(&tx)
                    .expect("Could not serialize VersionedTransaction"),
                Some(1),
            )
            .await?;
        info!("Txn sent success");
        i += 1;
    }

    Ok(())
}
