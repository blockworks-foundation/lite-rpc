use log::{debug, error, info, warn};
use solana_lite_rpc_cluster_endpoints::grpc_multiplex::{
    create_grpc_multiplex_blocks_subscription, create_grpc_multiplex_slots_subscription,
};
use solana_lite_rpc_cluster_endpoints::grpc_subscription_autoreconnect::{
    GrpcConnectionTimeouts, GrpcSourceConfig,
};
use solana_lite_rpc_core::structures::epoch::{EpochCache, EpochRef};
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::structures::slot_notification::SlotNotification;
use solana_lite_rpc_core::types::{BlockStream, SlotStream};
use solana_lite_rpc_history::block_stores::postgres::postgres_block_store_query::PostgresQueryBlockStore;
use solana_lite_rpc_history::block_stores::postgres::postgres_block_store_writer::PostgresBlockStore;
use solana_lite_rpc_history::block_stores::postgres::PostgresSessionConfig;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{env, process};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

const CHANNEL_SIZE_WARNING_THRESHOLD: usize = 5;
#[ignore = "need to enable postgres"]
#[tokio::test]
async fn storage_test() {
    // RUST_LOG=info,storage_integration_tests=debug,solana_lite_rpc_history=trace
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    configure_panic_hook();

    let no_pgconfig = env::var("PG_CONFIG").is_err();

    let pg_session_config = if no_pgconfig {
        info!("No PG_CONFIG env - use hartcoded defaults for integration test");
        PostgresSessionConfig::new_for_tests()
    } else {
        info!("PG_CONFIG env defined");
        PostgresSessionConfig::new_from_env().unwrap().unwrap()
    };

    let rpc_url = std::env::var("RPC_URL").expect("env var RPC_URL is mandatory");

    let rpc_client = Arc::new(RpcClient::new(rpc_url));

    let grpc_addr = env::var("GRPC_ADDR").expect("need grpc url for green");
    let grpc_x_token = env::var("GRPC_X_TOKEN").ok();

    info!(
        "Using grpc source on {} ({})",
        grpc_addr,
        grpc_x_token.is_some()
    );

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
    };

    let grpc_config = GrpcSourceConfig::new(grpc_addr, grpc_x_token, None, timeouts.clone());
    let grpc_sources = vec![grpc_config];

    let (slot_notifier, _jh_multiplex_slotstream) =
        create_grpc_multiplex_slots_subscription(grpc_sources.clone());

    let (blocks_notifier, _jh_multiplex_blockstream) =
        create_grpc_multiplex_blocks_subscription(grpc_sources);

    let (epoch_cache, _) = EpochCache::bootstrap_epoch(&rpc_client).await.unwrap();

    let block_storage_query = Arc::new(
        PostgresQueryBlockStore::new(epoch_cache.clone(), pg_session_config.clone()).await,
    );

    let block_storage = Arc::new(PostgresBlockStore::new(epoch_cache, pg_session_config).await);
    let current_epoch = rpc_client.get_epoch_info().await.unwrap().epoch;
    block_storage
        .drop_epoch_schema(EpochRef::new(current_epoch))
        .await
        .unwrap();
    block_storage
        .drop_epoch_schema(EpochRef::new(current_epoch).get_next_epoch())
        .await
        .unwrap();

    let (jh1_1, first_init) =
        storage_prepare_epoch_schema(slot_notifier.resubscribe(), block_storage.clone());
    // coordinate initial epoch schema creation
    first_init.cancelled().await;

    let jh1_2 = storage_listen(blocks_notifier.resubscribe(), block_storage.clone());
    let jh2 = block_debug_listen(blocks_notifier.resubscribe());
    let jh3 =
        spawn_client_to_blockstorage(block_storage_query.clone(), blocks_notifier.resubscribe());
    drop(blocks_notifier);

    let seconds_to_run = env::var("SECONDS_TO_RUN")
        .map(|s| s.parse::<u64>().expect("a number"))
        .unwrap_or(20);
    info!("Run tests for some time ({} seconds) ...", seconds_to_run);
    sleep(Duration::from_secs(seconds_to_run)).await;

    jh1_1.abort();
    jh1_2.abort();
    jh2.abort();
    jh3.abort();

    info!("Tests aborted forcefully by design.");
}

// TODO this is a race condition as the .save might get called before the schema was prepared
fn storage_prepare_epoch_schema(
    slot_notifier: SlotStream,
    postgres_storage: Arc<PostgresBlockStore>,
) -> (JoinHandle<()>, CancellationToken) {
    let mut debounce_slot = 0;
    let building_epoch_schema = CancellationToken::new();
    let first_run_signal = building_epoch_schema.clone();
    let join_handle = tokio::spawn(async move {
        let mut slot_notifier = slot_notifier;
        loop {
            match slot_notifier.recv().await {
                Ok(SlotNotification { processed_slot, .. }) => {
                    if processed_slot >= debounce_slot {
                        let created = postgres_storage
                            .prepare_epoch_schema(processed_slot)
                            .await
                            .unwrap();
                        first_run_signal.cancel();
                        debounce_slot = processed_slot + 64; // wait a bit before hammering the DB again
                        if created {
                            debug!("Async job prepared schema at slot {}", processed_slot);
                        } else {
                            debug!(
                                "Async job for preparing schema at slot {} was a noop",
                                processed_slot
                            );
                        }
                    }
                }
                _ => {
                    warn!("Error receiving slot - continue");
                }
            }
        }
    });
    (join_handle, building_epoch_schema)
}

/// run the optimizer at least every n slots
const OPTIMIZE_EVERY_N_SLOTS: u64 = 10;
/// wait at least n slots before running the optimizer again
const OPTIMIZE_DEBOUNCE_SLOTS: u64 = 4;

// note: the consumer lags far behind the ingress of blocks and transactions
fn storage_listen(
    block_notifier: BlockStream,
    block_storage: Arc<PostgresBlockStore>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_optimizer_run = 0;
        let mut block_notifier = block_notifier;
        // this is the critical write loop
        loop {
            match block_notifier.recv().await {
                Ok(block) => {
                    if block.commitment_config != CommitmentConfig::confirmed() {
                        debug!(
                            "Skip block {}@{} due to commitment level",
                            block.slot, block.commitment_config.commitment
                        );
                        continue;
                    }
                    let started = Instant::now();
                    debug!(
                        "Storage task received block: {}@{} with {} txs",
                        block.slot,
                        block.commitment_config.commitment,
                        block.transactions.len()
                    );

                    if block_notifier.len() > CHANNEL_SIZE_WARNING_THRESHOLD {
                        warn!(
                            "(soft_realtime) Block queue is growing - {} elements",
                            block_notifier.len()
                        );
                    }

                    // TODO we should intercept finalized blocks and try to update only the status optimistically

                    // avoid backpressure here!

                    block_storage.save_block(&block).await.unwrap();

                    // we should be faster than 150ms here
                    let elapsed = started.elapsed();
                    debug!(
                        "Successfully stored block {} to postgres which took {:.2}ms - remaining {} queue elements",
                        block.slot,
                        elapsed.as_secs_f64() * 1000.0, block_notifier.len()
                    );
                    if elapsed > Duration::from_millis(150) {
                        warn!("(soft_realtime) Write operation was slow!");
                    }

                    // debounce for 4 slots but run at least every 10 slots
                    if block.slot > last_optimizer_run + OPTIMIZE_EVERY_N_SLOTS
                        || block.slot > last_optimizer_run + OPTIMIZE_DEBOUNCE_SLOTS
                            && started.elapsed() < Duration::from_millis(200)
                            && block_notifier.is_empty()
                    {
                        debug!(
                            "Use extra time to do some optimization (slot {})",
                            block.slot
                        );
                        block_storage
                            .optimize_blocks_table(block.slot)
                            .await
                            .unwrap();
                        last_optimizer_run = block.slot;
                    }
                } // -- Ok
                Err(RecvError::Lagged(missed_blocks)) => {
                    warn!(
                        "Could not keep up with producer - missed {} blocks",
                        missed_blocks
                    );
                }
                Err(other_err) => {
                    warn!("Error receiving block: {:?}", other_err);
                }
            }

            // ...
        }
    })
}

fn block_debug_listen(block_notifier: BlockStream) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_highest_slot_number = 0;
        let mut block_notifier = block_notifier;

        loop {
            match block_notifier.recv().await {
                Ok(block) => {
                    debug!(
                        "Saw block: {}@{} with {} txs",
                        block.slot,
                        block.commitment_config.commitment,
                        block.transactions.len()
                    );

                    // check monotony
                    // note: this succeeds if poll_block parallelism is 1 (see NUM_PARALLEL_BLOCKS)
                    if block.commitment_config == CommitmentConfig::confirmed() {
                        if block.slot > last_highest_slot_number {
                            last_highest_slot_number = block.slot;
                        } else {
                            // note: ATM this fails very often (using the RPC poller)
                            warn!(
                                "Monotonic check failed - block {} is out of order, last highest was {}",
                                block.slot, last_highest_slot_number
                            );
                        }
                    }
                } // -- Ok
                Err(RecvError::Lagged(missed_blocks)) => {
                    warn!(
                        "Could not keep up with producer - missed {} blocks",
                        missed_blocks
                    );
                }
                Err(other_err) => {
                    panic!("Error receiving block: {:?}", other_err);
                }
            }

            // ...
        }
    })
}

fn spawn_client_to_blockstorage(
    block_storage_query: Arc<PostgresQueryBlockStore>,
    mut blocks_notifier: Receiver<ProducedBlock>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // note: no startup deloy
        loop {
            match blocks_notifier.recv().await {
                Ok(ProducedBlock {
                    slot,
                    commitment_config,
                    ..
                }) => {
                    if commitment_config != CommitmentConfig::confirmed() {
                        continue;
                    }
                    let confirmed_slot = slot;
                    // we cannot expect the most recent data
                    let query_slot = confirmed_slot - 3;
                    match block_storage_query.query_block(query_slot).await {
                        Ok(pb) => {
                            info!(
                                "Query result for slot {}: {}",
                                query_slot,
                                to_string_without_transactions(&pb)
                            );
                            for tx in pb.transactions.iter().take(10) {
                                info!("  - tx: {}", tx.signature);
                            }
                            if pb.transactions.len() > 10 {
                                info!("  - ... and {} more", pb.transactions.len() - 10);
                            }
                        }
                        Err(err) => {
                            info!("Query did not return produced block: {}", err);
                        }
                    }
                    // Query result for slot 245710738
                    // Inserting block 245710741 to schema rpc2a_epoch_581 postgres took 1.52ms
                }
                Err(_err) => {
                    warn!("Aborting client");
                    break;
                }
            }
            sleep(Duration::from_secs(1)).await;
        }
    })
}

fn to_string_without_transactions(produced_block: &ProducedBlock) -> String {
    format!(
        r#"
            leader_id={:?}
            blockhash={}
            block_height={}
            slot={}
            parent_slot={}
            block_time={}
            commitment_config={}
            previous_blockhash={}
            num_transactions={}
        "#,
        produced_block.leader_id,
        produced_block.blockhash,
        produced_block.block_height,
        produced_block.slot,
        produced_block.parent_slot,
        produced_block.block_time,
        produced_block.commitment_config.commitment,
        produced_block.previous_blockhash,
        produced_block.transactions.len(),
        // rewards
        // transactions
    )
}

fn configure_panic_hook() {
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        default_panic(panic_info);
        // e.g. panicked at 'BANG', lite-rpc/tests/blockstore_integration_tests:260:25
        error!("{}", panic_info);
        eprintln!("{}", panic_info);
        process::exit(12);
    }));
}
