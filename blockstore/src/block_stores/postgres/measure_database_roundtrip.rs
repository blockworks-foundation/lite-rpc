use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;
use log::{debug};
use tokio_postgres::Row;
use tracing::field::debug;
use crate::block_stores::postgres::{BlockstorePostgresSessionConfig, PostgresSession};

pub async fn measure_select1_roundtrip(n_connections: usize, pg_session_config: &BlockstorePostgresSessionConfig) -> (u32, Duration) {
    debug!("Measure database roundtrip with {} connections", n_connections);
    let counter = Arc::new(AtomicU32::new(0));

    let mut jh_tasks = vec![];
    for _session in 0..n_connections {
        let counter = counter.clone();
        let postgres_session_config = pg_session_config.clone();
        let jh = tokio::spawn(async move {
            let postgres_session = PostgresSession::new(postgres_session_config).await.unwrap();

            let started_at = tokio::time::Instant::now();
            // 100 sequenctial roundtrips
            const COUNT: usize = 1000;

            for j in 0..COUNT {
                let _result: Row = postgres_session.query_one("SELECT 1", &[]).await.unwrap();
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            (started_at.elapsed() / COUNT as u32)

        });
        jh_tasks.push(jh);
    }

    let mut timings = vec![];
    for jh in jh_tasks {
        let elapsed = jh.await.unwrap();
        timings.push(elapsed);
        debug!("elapsed: {:?}", elapsed);
    }
    let avg_time = timings.iter().sum::<Duration>() / timings.len() as u32;
    let num_queries = counter.load(std::sync::atomic::Ordering::Relaxed);
    debug!("total num queries: {}", num_queries);
    debug!("avg roundtrip: {:?}", avg_time);

    (num_queries, avg_time)
}
