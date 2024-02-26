use std::env;
use std::path::Path;
use std::sync::Arc;
use log::info;
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

    let block_file = Path::new("/Users/stefan/mango/code/lite-rpc/localdev-groovie-testing/blocks_on_disk/blocks-000254998xxx/block-000254998544-confirmed-1708964903356.dat");
    let produced_block = decode_from_dump(&block_file);

    block_storage.save_block(&produced_block).await.expect("write must succeed");


    info!("block written");

}
