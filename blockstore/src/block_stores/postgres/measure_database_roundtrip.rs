use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;
use log::{debug};
use tokio_postgres::Row;
use crate::block_stores::postgres::PostgresSession;

pub async fn measure_select1_roundtrip() -> (u32, Duration) {
    let counter = Arc::new(AtomicU32::new(0));

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

    let mut timings = vec![];
    for jh in jh_tasks {
        let elapsed = jh.await.unwrap();
        timings.push(elapsed);
        debug!("elapsed: {:?}", elapsed);
    }
    let avg_time = timings.iter().sum::<Duration>() / timings.len() as u32;
    let num_queries = counter.load(std::sync::atomic::Ordering::Relaxed);
    debug!("total num queris: {}", num_queries);
    debug!("avg roundtrip: {:?}", avg_time);

    (num_queries, avg_time)
}
