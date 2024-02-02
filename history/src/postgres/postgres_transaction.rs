use crate::postgres::postgres_epoch::PostgresEpoch;
use bytes::Bytes;
use futures_util::pin_mut;
use log::{debug, trace, warn};
use solana_lite_rpc_core::structures::epoch::EpochRef;
use solana_lite_rpc_core::{encoding::BASE64, structures::produced_block::TransactionInfo};
use solana_sdk::slot_history::Slot;
use tokio::time::Instant;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::CopyInSink;

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
        format!(include_str!("create_table_transactions.sql"),
            rpc2_schema = schema
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

    pub async fn save_transaction_copyin(
        postgres_session: PostgresSession,
        epoch: EpochRef,
        transactions: &[Self],
    ) -> anyhow::Result<bool> {
        let schema = PostgresEpoch::build_schema_name(epoch);

        let instant = Instant::now();
        // let temp_table = self.get_new_temp_table();
        postgres_session
            .execute_multiple(
                format!(
                    r#"CREATE TEMP TABLE IF NOT EXISTS transaction_raw_blockdata(
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
                    "#
                )
                    .as_str(),
            )
            .await?;

        let statement = format!(
            r#"
            COPY transaction_raw_blockdata(
                signature, slot, err, cu_requested, prioritization_fees, cu_consumed, recent_blockhash, message
            ) FROM STDIN BINARY
        "#,
        );
        let started_at = Instant::now();
        let sink: CopyInSink<bytes::Bytes> = postgres_session.copy_in(statement.as_str()).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[Type::TEXT, Type::INT8, Type::TEXT, Type::INT8, Type::INT8, Type::INT8, Type::TEXT, Type::TEXT],
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
                .await
                .unwrap();
        }

        let num_rows = writer.finish().await?;
        debug!(
            "inserted {} accounts for transaction into temp table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        let statement = format!(
            r#"
                SELECT
                    ( select transaction_id from {schema}.transaction_ids tx_lkup where tx_lkup.signature = new_amt_data.signature ),
                    slot, err, cu_requested, prioritization_fees, cu_consumed, recent_blockhash, message
                FROM transaction_raw_blockdata AS new_amt_data
        "#,
        );
        let started_at = Instant::now();
        let rows = postgres_session.execute(statement.as_str(), &[]).await?;

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
                FROM {schema}.transactions_todo
                WHERE slot = {}
            "#,
            slot,
            schema = schema
        );
        let _ = postgres_session.client.query(&statement, &[]).await;
        todo!()
    }
}
