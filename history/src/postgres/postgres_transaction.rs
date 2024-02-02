use crate::postgres::postgres_epoch::PostgresEpoch;
use bytes::Bytes;
use futures_util::pin_mut;
use log::{debug, trace, warn};
use solana_lite_rpc_core::structures::epoch::EpochRef;
use solana_lite_rpc_core::{encoding::BASE64, structures::produced_block::TransactionInfo};
use solana_sdk::slot_history::Slot;
use solana_sdk::transaction::{TransactionError, VersionedTransaction};
use tokio::time::Instant;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::CopyInSink;

use super::postgres_session::PostgresSession;

#[derive(Debug)]
pub struct PostgresTransaction {
    pub signature: String,
    // TODO clarify
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

    pub fn into_transaction_info(&self) -> TransactionInfo {
        TransactionInfo {
            signature: self.signature.clone(),
            err: self
                .err
                .as_ref()
                .map(|x| BASE64.deserialize::<TransactionError>(x).ok())
                .flatten(),
            cu_requested: self.cu_requested.map(|x| x as u32),
            prioritization_fees: self.prioritization_fees.map(|x| x as u64),
            cu_consumed: self.cu_consumed.map(|x| x as u64),
            recent_blockhash: self.recent_blockhash.clone(),
            message: self.message.clone(),
            // TODO readable_accounts etc.
            readable_accounts: vec![],
            writable_accounts: vec![],
            is_vote: false,
        }
    }

    pub fn build_create_table_statement(epoch: EpochRef) -> String {
        let schema = PostgresEpoch::build_schema_name(epoch);
        format!(include_str!("create_table_transactions.sql"),
            schema = schema
        )
    }

    // removed the foreign key as it slows down inserts
    pub fn build_foreign_key_statement(epoch: EpochRef) -> String {
        let schema = PostgresEpoch::build_schema_name(epoch);
        format!(
            r#"
                ALTER TABLE {schema}.transaction_blockdata
                ADD CONSTRAINT fk_transactions FOREIGN KEY (slot) REFERENCES {schema}.blocks (slot);
            "#,
            schema = schema
        )
    }

    pub async fn save_transactions_from_block(
        postgres_session: PostgresSession,
        epoch: EpochRef,
        transactions: &[Self],
    ) -> anyhow::Result<()> {
        let schema = PostgresEpoch::build_schema_name(epoch);

        let statmement = r#"
            CREATE TEMP TABLE IF NOT EXISTS transaction_raw_blockdata(
                signature text,
                slot bigint,
                err text,
                cu_requested bigint,
                prioritization_fees bigint,
                cu_consumed bigint,
                recent_blockhash text,
                message text
            );
            TRUNCATE transaction_raw_blockdata;
        "#;
        postgres_session.execute_multiple(statmement).await?;

        let statement = r#"
            COPY transaction_raw_blockdata(
                signature, slot, err, cu_requested, prioritization_fees, cu_consumed, recent_blockhash, message
            ) FROM STDIN BINARY
        "#;
        let started_at = Instant::now();
        let sink: CopyInSink<bytes::Bytes> = postgres_session.copy_in(statement).await?;
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
                Type::TEXT
            ],
        );
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

            writer
                .as_mut()
                .write(&[
                    &signature,
                    &slot,
                    &err,
                    &cu_requested,
                    &prioritization_fees,
                    &cu_consumed,
                    &recent_blockhash,
                    &message,
                ])
                .await?;
        }

        let num_rows = writer.finish().await?;
        debug!(
            "inserted {} raw transaction data rows into temp table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        let statement = format!(
            r#"
            INSERT INTO {schema}.transaction_ids(signature)
            SELECT signature from transaction_raw_blockdata
            ON CONFLICT DO NOTHING
            "#,
        );
        let started_at = Instant::now();
        let num_rows = postgres_session.execute(statement.as_str(), &[]).await?;
        debug!(
            "inserted {} signatures into transaction_ids table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        let statement = format!(
            r#"
                INSERT INTO {schema}.transaction_blockdata
                SELECT
                    ( SELECT transaction_id FROM {schema}.transaction_ids tx_lkup WHERE tx_lkup.signature = transaction_raw_blockdata.signature ),
                    slot, err, cu_requested, prioritization_fees, cu_consumed, recent_blockhash, message
                FROM transaction_raw_blockdata
        "#,
            schema = schema,
        );
        let started_at = Instant::now();
        let num_rows = postgres_session.execute(statement.as_str(), &[]).await?;
        debug!(
            "inserted {} rows into transaction block table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        Ok(())
    }

    pub fn build_query_statement(
        epoch: EpochRef,
        slot: Slot,
    ) -> String {
        format!(
            r#"
                SELECT
                    (SELECT signature FROM {schema}.transaction_ids tx_ids WHERE tx_ids.transaction_id = transaction_blockdata.transaction_id),
                    err, cu_requested, prioritization_fees, cu_consumed, recent_blockhash, message
                FROM {schema}.transaction_blockdata
                WHERE slot = {}
            "#,
            slot,
            schema = PostgresEpoch::build_schema_name(epoch),
        )
    }
}
