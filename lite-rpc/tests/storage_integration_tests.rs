use jsonrpsee::tracing::warn;
use log::{debug, error, info};
use solana_lite_rpc_cluster_endpoints::endpoint_stremers::EndpointStreaming;
use solana_lite_rpc_cluster_endpoints::json_rpc_subscription::create_json_rpc_polling_subscription;
use solana_lite_rpc_core::structures::epoch::EpochCache;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::traits::block_storage_interface::BlockStorageInterface;
use solana_lite_rpc_core::types::BlockStream;
use solana_lite_rpc_history::block_stores::postgres_block_store::PostgresBlockStore;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

#[tokio::test]
async fn storage_test() {
    // RUST_LOG=info,storage_integration_tests=debug,solana_lite_rpc_history=trace
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    configure_panic_hook();

    let rpc_url = std::env::var("RPC_URL").expect("env var RPC_URL is mandatory");

    let rpc_client = Arc::new(RpcClient::new(rpc_url));

    let (subscriptions, _cluster_endpoint_tasks) =
        create_json_rpc_polling_subscription(rpc_client.clone()).unwrap();

    let EndpointStreaming {
        blocks_notifier, ..
    } = subscriptions;

    let epoch_data = EpochCache::bootstrap_epoch(&rpc_client).await.unwrap();

    let block_storage = Arc::new(PostgresBlockStore::new(epoch_data).await);

    let jh1 = storage_listen(blocks_notifier.resubscribe(), block_storage.clone());
    let jh2 = block_debug_listen(blocks_notifier.resubscribe());
    drop(blocks_notifier);

    info!("Run tests for some time ...");
    sleep(Duration::from_secs(30)).await;

    jh1.abort();
    jh2.abort();

    info!("Tests aborted forcefully by design.");
}

// note: the consumer lags far behind the ingress of blocks and transactions
fn storage_listen(
    block_notifier: BlockStream,
    block_storage: Arc<dyn BlockStorageInterface>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut block_notifier = block_notifier;
        loop {
            match block_notifier.recv().await {
                Ok(block) => {
                    debug!(
                        "Received block: {} with {} txs",
                        block.slot,
                        block.transactions.len()
                    );

                    let produced_block = ProducedBlock {
                        transactions: block.transactions,
                        leader_id: block.leader_id,
                        blockhash: block.blockhash,
                        block_height: block.block_height,
                        slot: block.slot,
                        parent_slot: block.parent_slot,
                        block_time: block.block_time,
                        commitment_config: block.commitment_config,
                        previous_blockhash: block.previous_blockhash,
                        rewards: block.rewards,
                    };

                    let started = Instant::now();
                    block_storage.save(&produced_block).await.unwrap();
                    debug!(
                        "Saving block to postgres took {:.2}ms",
                        started.elapsed().as_secs_f64() * 1000.0
                    );
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

fn block_debug_listen(block_notifier: BlockStream) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut block_notifier = block_notifier;
        loop {
            match block_notifier.recv().await {
                Ok(block) => {
                    debug!(
                        "Saw block: {} with {} txs",
                        block.slot,
                        block.transactions.len()
                    );
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

fn configure_panic_hook() {
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        default_panic(panic_info);
        if let Some(location) = panic_info.location() {
            error!(
                "panic occurred in file '{}' at line {}",
                location.file(),
                location.line(),
            );
        } else {
            error!("panic occurred but can't get location information...");
        }
        // note: we do not exit the process to allow proper test execution
    }));
}
