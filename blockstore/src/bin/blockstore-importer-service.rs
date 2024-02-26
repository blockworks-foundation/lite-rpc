use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use log::{debug, info};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tracing_subscriber::EnvFilter;
use solana_lite_rpc_blockstore::block_stores::postgres::postgres_block_store_writer::PostgresBlockStore;
use solana_lite_rpc_blockstore::block_stores::postgres::PostgresSessionConfig;
use solana_lite_rpc_cluster_endpoints::foobar_decode_from_dump::decode_from_dump;
use solana_lite_rpc_core::structures::epoch::EpochCache;

#[tokio::main]
async fn main() {
    // RUST_LOG=info,storage_integration_tests=debug,solana_lite_rpc_blockstore=trace
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let blocks_list_file = env::var("BLOCKS_LIST_FILE").expect("env var BLOCKS_LIST_FILE is mandatory");
    let blocks_list_path = Path::new(blocks_list_file.as_str());
    assert!(blocks_list_path.is_file(), "File not found: {}", blocks_list_file);

    let no_pgconfig = env::var("PG_CONFIG").is_err();

    let pg_session_config = if no_pgconfig {
        info!("No PG_CONFIG env - use hartcoded defaults for integration test");
        PostgresSessionConfig::new_for_tests()
    } else {
        info!("PG_CONFIG env defined");
        PostgresSessionConfig::new_from_env().unwrap().unwrap()
    };

    let rpc_url = std::env::var("RPC_ADDR").expect("env var RPC_ADDR is mandatory");

    let rpc_client = Arc::new(RpcClient::new(rpc_url));

    let (epoch_cache, _) = EpochCache::bootstrap_epoch(&rpc_client).await.unwrap();

    let block_storage = Arc::new(PostgresBlockStore::new(epoch_cache, pg_session_config).await);

    let file = File::open(blocks_list_path).expect("file not found");
    let reader = BufReader::new(file);
    for line in reader.lines().map(|l| l.expect("Could not parse line")) {
        let block_file = Path::new(line.as_str());
        assert!(block_file.is_file(), "File not found: {}", block_file.to_str().unwrap());
        info!("reading file {:?} with {} bytes", block_file.to_str().unwrap(), block_file.metadata().unwrap().len());
        assert!(block_file.file_name().unwrap().to_str().unwrap().starts_with("block-"), "File name does not start with 'block-': {:?}", block_file.to_str().unwrap());
        let produced_block = decode_from_dump(&block_file);
        info!("saving block: {}", produced_block.slot);
        block_storage.save_block(&produced_block).await.expect("write must succeed");
    }

    info!("block written");

}
