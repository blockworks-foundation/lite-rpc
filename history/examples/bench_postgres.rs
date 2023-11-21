use std::env;
use std::sync::Arc;
use std::thread::{sleep, Thread};
use std::time::Duration;
use bytes::Bytes;
use futures_util::future::join_all;
use futures_util::{pin_mut, stream};
use itertools::Itertools;
use log::info;
use solana_sdk::blake3::Hash;
use solana_sdk::signature::Signature;
use tokio::time::Instant;
use tokio_postgres::{Client, CopyInSink};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use solana_lite_rpc_history::postgres::postgres_session::PostgresSession;

pub async fn copy_in(client: Arc<tokio_postgres::Client>) -> anyhow::Result<()> {

    let statement = format!(
        r#"
                COPY public.transactions_copyin(
                    signature, slot, err, cu_requested, prioritization_fees, cu_consumed, recent_blockhash, message
                ) FROM STDIN BINARY
            "#
    );

    // BinaryCopyInWriter
    // https://github.com/sfackler/rust-postgres/blob/master/tokio-postgres/tests/test/binary_copy.rs
    let sink: CopyInSink<Bytes> = client.copy_in(&statement).await.unwrap();

    // signature text NOT NULL,
    // slot bigint NOT NULL,
    // err text ,
    // cu_requested bigint,
    // prioritization_fees bigint,
    // cu_consumed bigint,
    // recent_blockhash text NOT NULL,
    // message text NOT NULL

    let sig = Signature::new_unique().to_string();
    let slot = 200_000_000 as i64;
    let err = None::<&str>;
    let cu_requested = None::<i64>;
    let prioritization_fees = None::<i64>;
    let cu_consumed = None::<i64>;
    let recent_blockhash = Hash::new(&[1u8; 32]).to_string();
    let message = "";


    let started = Instant::now();
    let writer = BinaryCopyInWriter::new(sink, &[Type::TEXT, Type::INT8, Type::TEXT, Type::INT8, Type::INT8, Type::INT8, Type::TEXT, Type::TEXT]);
    pin_mut!(writer);

    const count: usize = 10000;
    for i in 0..count {
        let slot_x = slot + i as i64;
        writer.as_mut().write(&[&sig, &slot_x, &err, &cu_requested, &prioritization_fees, &cu_consumed, &recent_blockhash, &message]).await.unwrap();
    }

    // writer.as_mut().write(&[&"foobar", &99i64, &"foobar", &99i64, &99i64, &99i64, &"foobar", &"foobar"]).await.unwrap();

    writer.finish().await.unwrap();


    info!("wrote {} rows in {:02}ms", count, started.elapsed().as_secs_f64() * 1000.0);

    Ok(())
}

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();
    env::set_var("PG_CONFIG", "host=localhost port=5433 dbname=stefan user=postgres sslmode=disable");

    let write_session_single = PostgresSession::new_from_env().await.unwrap();
    // let write_session1 = PostgresSession::new_from_env().await.unwrap();
    // let write_session2 = PostgresSession::new_from_env().await.unwrap();
    // let write_session3 = PostgresSession::new_from_env().await.unwrap();
    // let write_session4 = PostgresSession::new_from_env().await.unwrap();
    // let write_session5 = PostgresSession::new_from_env().await.unwrap();

    let mut write_sessions = Vec::new();
    for _i in 0..5 {
        write_sessions.push(PostgresSession::new_from_env().await.unwrap());
    }

    let started = Instant::now();
    // let (ret1, ret2, ret3, ret4, ret5) = futures_util::join!(
    //     copy_in(&write_session1.client),
    //     copy_in(&write_session2.client),
    //     copy_in(&write_session3.client),
    //     copy_in(&write_session4.client),
    //     copy_in(&write_session5.client),
    // );
    let queries = write_sessions.iter().map(|x| copy_in(x.client.clone())).collect_vec();
    let all_results: Vec<anyhow::Result<()>> = join_all(queries).await;

    // for result in all_results {
    //     result.unwrap();
    // }

    info!("parallel write rows in {:02}ms", started.elapsed().as_secs_f64() * 1000.0);

    let dummy_session = PostgresSession::new_from_env().await.unwrap();
    let rows = dummy_session.client
        .query("SELECT signature, slot, err, cu_requested, prioritization_fees, cu_consumed, recent_blockhash, message FROM public.transactions_copyin", &[])
        .await
        .unwrap();

    info!("total: {}", rows.len());

    for (i, row) in rows.iter().take(10).enumerate() {
        let sig = row.get::<_, &str>(0);
        let slot = row.get::<_, i64>(1);
        info!("row #{}: {} {}", i, sig, slot);
    }

}
