use bytes::Bytes;
use futures_util::future::join_all;
use futures_util::pin_mut;
use itertools::Itertools;
use log::info;
use solana_lite_rpc_history::postgres::postgres_session::{PostgresSession, PostgresWriteSession};
use solana_sdk::blake3::Hash;
use solana_sdk::signature::Signature;
use std::sync::Arc;
use tokio::time::Instant;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
use tokio_postgres::{CopyInSink, GenericClient};
use solana_lite_rpc_history::postgres::postgres_config::PostgresSessionConfig;

pub async fn copy_in(client: &tokio_postgres::Client) -> anyhow::Result<()> {
    let statement = format!(
        r#"
                COPY transactions_copyin(
                    signature, slot, err, cu_requested, prioritization_fees, cu_consumed, recent_blockhash, message
                ) FROM STDIN BINARY
            "#
    );

    // BinaryCopyInWriter
    // https://github.com/sfackler/rust-postgres/blob/master/tokio-postgres/tests/test/binary_copy.rs
    let sink: CopyInSink<Bytes> = client.copy_in(&statement).await.unwrap();

    let sig = Signature::new_unique().to_string();
    let slot = 200_000_000 as i64;
    let err = None::<&str>;
    let cu_requested = None::<i64>;
    let prioritization_fees = None::<i64>;
    let cu_consumed = None::<i64>;
    let recent_blockhash = Hash::new(&[1u8; 32]).to_string();
    let message = "";

    let started = Instant::now();
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::TEXT,
            Type::INT8,
            Type::TEXT,
            Type::INT8,
            Type::INT8,
            Type::INT8,
            Type::TEXT,
            Type::TEXT,
        ],
    );
    pin_mut!(writer);

    const COUNT: usize = 100000;
    for i in 0..COUNT {
        let slot_x = slot + i as i64;
        writer
            .as_mut()
            .write(&[
                &sig,
                &slot_x,
                &err,
                &cu_requested,
                &prioritization_fees,
                &cu_consumed,
                &recent_blockhash,
                &message,
            ])
            .await
            .unwrap();
    }

    writer.finish().await.unwrap();

    info!(
        "wrote {} rows in {:.2}ms",
        COUNT,
        started.elapsed().as_secs_f64() * 1000.0
    );

    Ok(())
}

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let pg_session_config = PostgresSessionConfig::new_for_tests();
    let session = PostgresWriteSession::new(pg_session_config).await.unwrap().get_write_session().await;

    let ddl_statement =
        r#"
        CREATE TEMP TABLE transactions_copyin
        (
            signature text NOT NULL,
            slot bigint NOT NULL,
            err text ,
            cu_requested bigint,
            prioritization_fees bigint,
            cu_consumed bigint,
            recent_blockhash text NOT NULL,
            message text NOT NULL
         )
        "#;

    session.execute(ddl_statement, &[]).await.unwrap();

    let row_count_before = count_rows(session.client.clone()).await;

    let started = Instant::now();

    copy_in(session.client.as_ref()).await.unwrap();

    info!(
        "copyin write rows in {:.2}ms",
        started.elapsed().as_secs_f64() * 1000.0
    );

    let row_count_after = count_rows(session.client.clone()).await;
    info!("total: {}", row_count_after);
    info!("inserted: {}", row_count_after - row_count_before);
}

async fn count_rows(client: Arc<tokio_postgres::Client>) -> i64 {
    let row = client
        .query_one("SELECT count(*) FROM transactions_copyin", &[])
        .await
        .unwrap();

    let count = row.get::<_, i64>(0);
    count
}
