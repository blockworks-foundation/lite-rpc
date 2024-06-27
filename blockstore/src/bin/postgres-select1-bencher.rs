use log::info;
use solana_lite_rpc_blockstore::block_stores::postgres::measure_database_roundtrip::measure_select1_roundtrip;

// RUST_LOG=info
// requires BLOCKSTOREDB_PG_CONFIG
#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    tracing_subscriber::fmt::init();

    let (num_queries, avg_time) = measure_select1_roundtrip().await;

    info!("total num queris: {}", num_queries);
    info!("avg roundtrip: {:.2?}", avg_time);

}
