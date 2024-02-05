use itertools::Itertools;
///
/// test program to query postgres the simples possible way
///
use log::info;
use solana_lite_rpc_history::block_stores::postgres::PostgresSession;
use solana_lite_rpc_history::block_stores::postgres::PostgresSessionConfig;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let pg_session_config = PostgresSessionConfig::new_for_tests();

    let single_session = PostgresSession::new(pg_session_config.clone())
        .await
        .unwrap();
    // run one query
    query_database_simple(single_session).await;
    info!("single query test ... done");

    // run parallel queries
    parallel_queries(pg_session_config).await;
    info!("parallel queries test ... done");

    Ok(())
}

async fn parallel_queries(pg_session_config: PostgresSessionConfig) {
    let many_sessions = vec![
        PostgresSession::new(pg_session_config.clone())
            .await
            .unwrap(),
        PostgresSession::new(pg_session_config.clone())
            .await
            .unwrap(),
        PostgresSession::new(pg_session_config.clone())
            .await
            .unwrap(),
        PostgresSession::new(pg_session_config.clone())
            .await
            .unwrap(),
        PostgresSession::new(pg_session_config.clone())
            .await
            .unwrap(),
        PostgresSession::new(pg_session_config.clone())
            .await
            .unwrap(),
        PostgresSession::new(pg_session_config.clone())
            .await
            .unwrap(),
        PostgresSession::new(pg_session_config.clone())
            .await
            .unwrap(),
    ];

    let futures = (0..many_sessions.len())
        .map(|si| {
            let session = many_sessions[si].clone();
            query_database_simple(session)
        })
        .collect_vec();

    futures_util::future::join_all(futures).await;
}

async fn query_database_simple(postgres_session: PostgresSession) {
    let statement = "SELECT 1";

    let started = tokio::time::Instant::now();
    let result = postgres_session.query_list(statement, &[]).await.unwrap();
    let elapsed = started.elapsed().as_secs_f64();
    info!(
        "num_rows: {} (took {:.2}ms)",
        result.len(),
        elapsed * 1000.0
    );
}
