use std::time::Instant;
use bytes::Bytes;
use futures_util::pin_mut;
use crate::postgres::postgres_epoch::PostgresEpoch;
use log::{debug, info, trace, warn};
use solana_sdk::blake3::Hash;
use solana_sdk::signature::Signature;
use solana_lite_rpc_core::structures::epoch::EpochRef;
use solana_lite_rpc_core::{encoding::BASE64, structures::produced_block::TransactionInfo};
use solana_sdk::slot_history::Slot;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::CopyInSink;
use tokio_postgres::types::{ToSql, Type};

use super::postgres_session::PostgresSession;

#[derive(Debug)]
pub struct PostgresTransaction {
    pub signature: String,
    pub slot: i64,
    pub err: Option<String>,
    pub cu_requested: Option<i64>,
    pub prioritization_fees: Option<i64>,
    pub cu_consumed: Option<i64>,
    pub recent_blockhash: String,
    pub message: String,
}

impl PostgresTransaction {
    pub fn new(value: &TransactionInfo, slot: Slot) -> Self {
        Self {
            signature: value.signature.clone(),
            err: value
                .err
                .clone()
                .map(|x| BASE64.serialize(&x).ok())
                .unwrap_or(None),
            cu_requested: value.cu_requested.map(|x| x as i64),
            prioritization_fees: value.prioritization_fees.map(|x| x as i64),
            cu_consumed: value.cu_consumed.map(|x| x as i64),
            recent_blockhash: value.recent_blockhash.clone(),
            message: value.message.clone(),
            slot: slot as i64,
        }
    }

    pub fn build_create_table_statement(epoch: EpochRef) -> String {
        let schema = PostgresEpoch::build_schema_name(epoch);
        format!(
            r#"
                CREATE TABLE IF NOT EXISTS {schema}.transactions (
                    signature VARCHAR(88) NOT NULL,
                    slot BIGINT NOT NULL,
                    err TEXT,
                    cu_requested BIGINT,
                    prioritization_fees BIGINT,
                    cu_consumed BIGINT,
                    recent_blockhash TEXT NOT NULL,
                    message TEXT NOT NULL,
                    CONSTRAINT pk_transaction_sig PRIMARY KEY(signature)
                  ) WITH (FILLFACTOR=90);
                  CREATE INDEX idx_slot ON {schema}.transactions USING btree (slot) WITH (FILLFACTOR=90);
                  CLUSTER {schema}.transactions USING idx_slot;
            "#,
            schema = schema
        )
    }

    pub fn build_foreign_key_statement(epoch: EpochRef) -> String {
        let schema = PostgresEpoch::build_schema_name(epoch);
        format!(
            r#"
                ALTER TABLE {schema}.transactions
                ADD CONSTRAINT fk_transactions FOREIGN KEY (slot) REFERENCES {schema}.blocks (slot);
            "#,
            schema = schema
        )
    }

    // this version uses INSERT statements
    pub async fn save_transaction_insert(
        postgres_session: &PostgresSession,
        epoch: EpochRef,
        slot: Slot,
        transactions: &[Self],
    ) -> anyhow::Result<()> {
        const NB_ARGUMENTS: usize = 8;
        let tx_count = transactions.len();
        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(NB_ARGUMENTS * tx_count);

        for tx in transactions.iter() {
            let PostgresTransaction {
                signature,
                slot,
                err,
                cu_requested,
                prioritization_fees,
                cu_consumed,
                recent_blockhash,
                message,
            } = tx;

            args.push(signature);
            args.push(slot);
            args.push(err);
            args.push(cu_requested);
            args.push(prioritization_fees);
            args.push(cu_consumed);
            args.push(recent_blockhash);
            args.push(message);
        }

        let values = PostgresSession::values_vecvec(NB_ARGUMENTS, tx_count, &[]);
        let schema = PostgresEpoch::build_schema_name(epoch);
        let statement = format!(
            r#"
                INSERT INTO {schema}.transactions
                (signature, slot, err, cu_requested, prioritization_fees, cu_consumed, recent_blockhash, message)
                VALUES {}
                ON CONFLICT DO NOTHING
            "#,
            values,
            schema = schema,
        );

        let inserted = postgres_session.execute_prepared(&statement, &args).await? as usize;

        if inserted < tx_count {
            warn!("Some ({}) transactions already existed and where not updated of {} total in schema {schema}",
                transactions.len() - inserted, transactions.len(), schema = schema);
        }

        trace!(
            "Inserted {} transactions chunk into epoch schema {} for block {}",
            inserted, schema, slot
        );

        Ok(())
    }

    // this version uses "COPY IN"
    pub async fn save_transaction_copyin(
        postgres_session: &PostgresSession,
        epoch: EpochRef,
        transactions: &[Self],
    ) -> anyhow::Result<bool> {

        let schema = PostgresEpoch::build_schema_name(epoch);
        let statement = format!(
            r#"
                COPY {schema}.transactions(
                    signature, slot, err, cu_requested, prioritization_fees, cu_consumed, recent_blockhash, message
                ) FROM STDIN BINARY
            "#,
            schema = schema,
        );

        // BinaryCopyInWriter
        // https://github.com/sfackler/rust-postgres/blob/master/tokio-postgres/tests/test/binary_copy.rs
        let sink: CopyInSink<Bytes> = postgres_session.copy_in(&statement).await.unwrap();

        let started = Instant::now();
        let writer = BinaryCopyInWriter::new(sink, &[Type::TEXT, Type::INT8, Type::TEXT, Type::INT8, Type::INT8, Type::INT8, Type::TEXT, Type::TEXT]);
        pin_mut!(writer);

        for tx in transactions {
            let PostgresTransaction {
                signature,
                slot,
                err,
                cu_requested,
                prioritization_fees,
                cu_consumed,
                recent_blockhash,
                message,
            } = tx;

            writer.as_mut().write(&[&signature, &slot, &err, &cu_requested, &prioritization_fees, &cu_consumed, &recent_blockhash, &message]).await.unwrap();
        }

        writer.finish().await.unwrap();

        Ok(true)
    }



    pub async fn get(
        postgres_session: PostgresSession,
        schema: &String,
        slot: Slot,
    ) -> Vec<TransactionInfo> {
        let statement = format!(
            r#"
                SELECT signature, err, cu_requested, prioritization_fees, cu_consumed, recent_blockhash, message
                FROM {schema}.transactions
                WHERE slot = {}
            "#,
            slot,
            schema = schema
        );
        let _ = postgres_session.client.query(&statement, &[]).await;
        todo!()
    }
}
