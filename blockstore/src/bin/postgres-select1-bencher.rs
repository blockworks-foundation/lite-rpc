use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use itertools::join;
use log::info;
use tokio::join;
use tokio_postgres::Row;
use solana_lite_rpc_blockstore::block_stores::postgres::{BlockstorePostgresSessionConfig, PostgresSession};

// RUST_LOG=info
// requires BLOCKSTOREDB_PG_CONFIG
#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() {
    tracing_subscriber::fmt::init();

    let mut counter = Arc::new(AtomicU32::new(0));

    let mut jh_tasks = vec![];
    for _session in 0..5 {
        let counter = counter.clone();
        let jh = tokio::spawn(async move {
            let postgres_session = PostgresSession::new_from_env().await.unwrap();

            let started_at = tokio::time::Instant::now();
            // 100 sequenctial roundtrips
            for j in 0..1000 {
                let _result: Row = postgres_session.query_one("SELECT 1", &[]).await.unwrap();
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            (started_at.elapsed())

        });
        jh_tasks.push(jh);
    }

    for jh in jh_tasks {
        let elapsed = jh.await.unwrap();
        info!("elapsed: {:?}", elapsed);
    }
    info!("total counter: {}", counter.load(std::sync::atomic::Ordering::Relaxed));


}
