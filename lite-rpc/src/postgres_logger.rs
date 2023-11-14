use anyhow::bail;
use chrono::{DateTime, Utc};
use futures::join;
use log::{info, warn};
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use solana_lite_rpc_core::{
    structures::notifications::{
        NotificationMsg, NotificationReciever, TransactionNotification,
        TransactionUpdateNotification,
    },
    AnyhowJoinHandle,
};
use solana_lite_rpc_history::postgres::postgres_session::{PostgresSession, PostgresSessionCache};
use std::time::Duration;
use tokio_postgres::types::ToSql;

lazy_static::lazy_static! {
    pub static ref MESSAGES_IN_POSTGRES_CHANNEL: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_messages_in_postgres", "Number of messages in postgres")).unwrap();
    pub static ref POSTGRES_SESSION_ERRORS: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_session_errors", "Number of failures while establishing postgres session")).unwrap();
}

use std::convert::From;

const MAX_QUERY_SIZE: usize = 200_000; // 0.2 mb

pub trait SchemaSize {
    const DEFAULT_SIZE: usize = 0;
    const MAX_SIZE: usize = 0;
}

#[derive(Debug)]
pub struct PostgresTx {
    pub signature: String,                   // 88 bytes
    pub recent_slot: i64,                    // 8 bytes
    pub forwarded_slot: i64,                 // 8 bytes
    pub forwarded_local_time: DateTime<Utc>, // 8 bytes
    pub processed_slot: Option<i64>,
    pub cu_consumed: Option<i64>,
    pub cu_requested: Option<i64>,
    pub quic_response: i16, // 2 bytes
}

impl SchemaSize for PostgresTx {
    const DEFAULT_SIZE: usize = 88 + (3 * 8) + 2;
    const MAX_SIZE: usize = Self::DEFAULT_SIZE + (3 * 8);
}

impl From<&TransactionNotification> for PostgresTx {
    fn from(value: &TransactionNotification) -> Self {
        Self {
            signature: value.signature.clone(),
            recent_slot: value.recent_slot as i64,
            forwarded_slot: value.forwarded_slot as i64,
            forwarded_local_time: value.forwarded_local_time,
            processed_slot: value.processed_slot.map(|x| x as i64),
            cu_consumed: value.cu_consumed.map(|x| x as i64),
            cu_requested: value.cu_requested.map(|x| x as i64),
            quic_response: value.quic_response,
        }
    }
}

#[derive(Debug)]
pub struct PostgresTxUpdate {
    pub signature: String,   // 88 bytes
    pub processed_slot: i64, // 8 bytes
    pub cu_consumed: Option<i64>,
    pub cu_requested: Option<i64>,
    pub cu_price: Option<i64>,
}

impl SchemaSize for PostgresTxUpdate {
    const DEFAULT_SIZE: usize = 88 + 8;
    const MAX_SIZE: usize = Self::DEFAULT_SIZE + (3 * 8);
}

impl From<&TransactionUpdateNotification> for PostgresTxUpdate {
    fn from(value: &TransactionUpdateNotification) -> Self {
        Self {
            signature: value.signature.clone(),
            processed_slot: value.slot as i64,
            cu_consumed: value.cu_consumed.map(|x| x as i64),
            cu_requested: value.cu_requested.map(|x| x as i64),
            cu_price: value.cu_price.map(|x| x as i64),
        }
    }
}

#[derive(Debug)]
pub struct AccountAddr {
    pub id: u32,
    pub addr: String,
}

const fn get_max_safe_inserts<T: SchemaSize>() -> usize {
    if T::DEFAULT_SIZE == 0 {
        panic!("DEFAULT_SIZE can't be 0. SchemaSize impl should override the DEFAULT_SIZE const");
    }

    MAX_QUERY_SIZE / T::DEFAULT_SIZE
}

const fn get_max_safe_updates<T: SchemaSize>() -> usize {
    if T::MAX_SIZE == 0 {
        panic!("MAX_SIZE can't be 0. SchemaSize impl should override the MAX_SIZE const");
    }

    MAX_QUERY_SIZE / T::MAX_SIZE
}

async fn send_txs(postgres_session: &PostgresSession, txs: &[PostgresTx]) -> anyhow::Result<()> {
    const NUMBER_OF_ARGS: usize = 8;

    if txs.is_empty() {
        return Ok(());
    }

    let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(NUMBER_OF_ARGS * txs.len());

    for tx in txs.iter() {
        let PostgresTx {
            signature,
            recent_slot,
            forwarded_slot,
            forwarded_local_time,
            processed_slot,
            cu_consumed,
            cu_requested,
            quic_response,
        } = tx;

        args.push(signature);
        args.push(recent_slot);
        args.push(forwarded_slot);
        args.push(forwarded_local_time);
        args.push(processed_slot);
        args.push(cu_consumed);
        args.push(cu_requested);
        args.push(quic_response);
    }

    let mut query = String::from(
        r#"
            INSERT INTO lite_rpc.Txs 
            (signature, recent_slot, forwarded_slot, forwarded_local_time, processed_slot, cu_consumed, cu_requested, quic_response)
            VALUES
        "#,
    );

    PostgresSession::multiline_query(&mut query, NUMBER_OF_ARGS, txs.len(), &[]);

    postgres_session.client.execute(&query, &args).await?;

    Ok(())
}

async fn update_txs(
    postgres_session: &PostgresSession,
    txs: &[PostgresTxUpdate],
) -> anyhow::Result<()> {
    const NUMBER_OF_ARGS: usize = 5;

    if txs.is_empty() {
        return Ok(());
    }

    let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(NUMBER_OF_ARGS * txs.len());

    for tx in txs.iter() {
        let PostgresTxUpdate {
            signature,
            processed_slot,
            cu_consumed,
            cu_requested,
            cu_price,
        } = tx;

        args.push(signature);
        args.push(processed_slot);
        args.push(cu_consumed);
        args.push(cu_requested);
        args.push(cu_price);
    }

    let mut query = String::from(
        r#"
            UPDATE lite_rpc.Txs AS t1 SET
                processed_slot  = t2.processed_slot,
                cu_consumed = t2.cu_consumed,
                cu_requested = t2.cu_requested,
                cu_price = t2.cu_price
            FROM (VALUES
        "#,
    );

    PostgresSession::multiline_query(
        &mut query,
        NUMBER_OF_ARGS,
        txs.len(),
        &["text", "bigint", "bigint", "bigint", "bigint"],
    );

    query.push_str(
        r#"
            ) AS t2(signature, processed_slot, cu_consumed, cu_requested, cu_price)
            WHERE t1.signature = t2.signature
        "#,
    );

    postgres_session.execute(&query, &args).await?;

    Ok(())
}

pub struct PostgresLogger {}

impl PostgresLogger {
    pub fn start(
        postgres_session_cache: PostgresSessionCache,
        mut recv: NotificationReciever,
    ) -> AnyhowJoinHandle {
        tokio::spawn(async move {
            info!("start postgres worker");

            const TX_MAX_CAPACITY: usize = get_max_safe_inserts::<PostgresTx>();
            const UPDATE_MAX_CAPACITY: usize = get_max_safe_updates::<PostgresTxUpdate>();

            let mut tx_batch: Vec<PostgresTx> = Vec::with_capacity(TX_MAX_CAPACITY);
            let mut update_batch = Vec::<PostgresTxUpdate>::with_capacity(UPDATE_MAX_CAPACITY);

            let mut session_establish_error = false;

            loop {
                // drain channel until we reach max capacity for any statement type
                loop {
                    if session_establish_error {
                        break;
                    }

                    // check for capacity
                    if tx_batch.len() >= TX_MAX_CAPACITY
                        || update_batch.len() >= UPDATE_MAX_CAPACITY
                    {
                        break;
                    }

                    match recv.try_recv() {
                        Ok(msg) => {
                            MESSAGES_IN_POSTGRES_CHANNEL.dec();

                            match msg {
                                NotificationMsg::TxNotificationMsg(tx) => {
                                    let mut tx = tx.iter().map(|x| x.into()).collect::<Vec<_>>();
                                    tx_batch.append(&mut tx)
                                }
                                NotificationMsg::BlockNotificationMsg(_) => {
                                    // ignore block storage as it has been moved to persistant history.
                                    continue;
                                }
                                NotificationMsg::UpdateTransactionMsg(update) => {
                                    let mut update = update.iter().map(|x| x.into()).collect();
                                    update_batch.append(&mut update)
                                }

                                NotificationMsg::AccountAddrMsg(_) => todo!(),
                            }
                        }
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                            log::error!("Postgres channel broke");
                            bail!("Postgres channel broke")
                        }
                    }
                }

                // if there's nothing to do, yield for a brief time
                if tx_batch.is_empty() && update_batch.is_empty() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }

                // Establish session with postgres or get an existing one
                let session = postgres_session_cache.get_session().await;
                session_establish_error = session.is_err();

                let Ok(session) = session else {
                    POSTGRES_SESSION_ERRORS.inc();

                    const TIME_OUT: Duration = Duration::from_millis(1000);
                    warn!("Unable to get postgres session. Retrying in {TIME_OUT:?}");
                    tokio::time::sleep(TIME_OUT).await;

                    continue;
                };

                POSTGRES_SESSION_ERRORS.set(0);

                // write to database when a successful connection is made
                let (res_txs, res_update) = join!(
                    send_txs(&session, &tx_batch),
                    update_txs(&session, &update_batch)
                );

                // clear batches only if results were successful
                if let Err(err) = res_txs {
                    warn!(
                        "Error sending tx batch ({:?}) to postgres {err:?}",
                        tx_batch.len()
                    );
                } else {
                    tx_batch.clear();
                }
                if let Err(err) = res_update {
                    warn!(
                        "Error sending update batch ({:?}) to postgres {err:?}",
                        update_batch.len()
                    );
                } else {
                    update_batch.clear();
                }
            }
        })
    }
}
