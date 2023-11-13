use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use jsonrpsee::tracing::field::debug;
use jsonrpsee::tracing::warn;
use log::{debug, error, info, Level, trace};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;
use solana_lite_rpc_cluster_endpoints::endpoint_stremers::EndpointStreaming;
use solana_lite_rpc_cluster_endpoints::json_rpc_subscription::create_json_rpc_polling_subscription;
use solana_lite_rpc_core::structures::epoch::EpochCache;
use solana_lite_rpc_core::structures::produced_block::{ProducedBlock, TransactionInfo};
use solana_lite_rpc_core::traits::block_storage_interface::BlockStorageInterface;
use solana_lite_rpc_core::types::BlockStream;
use solana_lite_rpc_history::block_stores::inmemory_block_store::InmemoryBlockStore;
use solana_lite_rpc_history::block_stores::multiple_strategy_block_store::MultipleStrategyBlockStorage;
use solana_lite_rpc_history::block_stores::postgres_block_store::PostgresBlockStore;
use solana_lite_rpc_history::history::History;

#[tokio::test]
async fn storage_test() {
    // RUST_LOG=info,storage_integration_tests=debug,solana_lite_rpc_history=trace
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    configure_panic_hook();

    let rpc_url = std::env::var("RPC_URL").expect("env var RPC_URL is mandatory");

    let rpc_client = Arc::new(RpcClient::new(rpc_url));

    let (subscriptions, cluster_endpoint_tasks) = create_json_rpc_polling_subscription(rpc_client.clone()).unwrap();

    let EndpointStreaming {
        blocks_notifier,
        cluster_info_notifier,
        slot_notifier,
        vote_account_notifier,
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


fn storage_listen(block_notifier: BlockStream, block_storage: Arc<dyn BlockStorageInterface>) -> JoinHandle<()> {
    let block_cache_jh = tokio::spawn(async move {
        let mut block_notifier = block_notifier;
        loop {
            match block_notifier.recv().await {
                Ok(block) => {
                    debug!("Received block: {} with {} txs", block.slot, block.transactions.len());

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
                    debug!("Saving block to postgres took {:.2}ms", started.elapsed().as_secs_f64() * 1000.0);
                } // -- Ok
                Err(RecvError::Lagged(missed_blocks)) => {
                    warn!("Could not keep up with producer - missed {} blocks", missed_blocks);
                }
                Err(other_err) => {
                    panic!("Error receiving block: {:?}", other_err);
                }
            }


            // ...
        }
    });

    block_cache_jh

}



fn block_debug_listen(block_notifier: BlockStream) -> JoinHandle<()> {
    let block_cache_jh = tokio::spawn(async move {
        let mut block_notifier = block_notifier;
        loop {
            match block_notifier.recv().await {
                Ok(block) => {
                    debug!("Saw block: {} with {} txs", block.slot, block.transactions.len());
                } // -- Ok
                Err(RecvError::Lagged(missed_blocks)) => {
                    warn!("Could not keep up with producer - missed {} blocks", missed_blocks);
                }
                Err(other_err) => {
                    panic!("Error receiving block: {:?}", other_err);
                }
            }


            // ...
        }
    });

    block_cache_jh

}


fn create_test_block() -> ProducedBlock {

    let sig1 = Signature::from_str("5VBroA4MxsbZdZmaSEb618WRRwhWYW9weKhh3md1asGRx7nXDVFLua9c98voeiWdBE7A9isEoLL7buKyaVRSK1pV").unwrap();
    let sig2 = Signature::from_str("3d9x3rkVQEoza37MLJqXyadeTbEJGUB6unywK4pjeRLJc16wPsgw3dxPryRWw3UaLcRyuxEp1AXKGECvroYxAEf2").unwrap();

    ProducedBlock {
        block_height: 42,
        blockhash: "blockhash".to_string(),
        previous_blockhash: "previous_blockhash".to_string(),
        parent_slot: 666,
        slot: 223555999,
        transactions: vec![
            create_test_tx(sig1),
            create_test_tx(sig2),
        ],
        // TODO double if this is unix millis or seconds
        block_time: 1699260872000,
        commitment_config: CommitmentConfig::finalized(),
        leader_id: None,
        rewards: None,
    }
}


fn create_test_tx(signature: Signature) -> TransactionInfo {
    TransactionInfo {
        signature: signature.to_string(),
        err: None,
        cu_requested: Some(40000),
        prioritization_fees: Some(5000),
        cu_consumed: Some(32000),
        recent_blockhash: "recent_blockhash".to_string(),
        message: "some message".to_string(),
    }
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

