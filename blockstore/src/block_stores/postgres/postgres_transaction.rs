use std::collections::HashSet;
use std::str::FromStr;

use futures_util::pin_mut;
use itertools::Itertools;
use log::debug;
use solana_lite_rpc_core::encoding::BinaryEncoding;
use solana_lite_rpc_core::solana_utils::hash_from_str;
use solana_lite_rpc_core::structures::epoch::EpochRef;
use solana_lite_rpc_core::{encoding::BASE64, structures::produced_block::TransactionInfo};
use solana_sdk::signature::Signature;
use solana_sdk::slot_history::Slot;
use solana_sdk::transaction::TransactionError;
use tokio::time::Instant;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
use tokio_postgres::CopyInSink;

use super::postgres_epoch::*;
use super::postgres_session::*;

#[derive(Debug)]
pub struct PostgresTransaction {
    pub signature: String,
    // TODO clarify
    pub slot: i64,
    pub cu_consumed: Option<i64>,
    pub cu_requested: Option<i64>,
    pub prioritization_fees: Option<i64>,
    pub recent_blockhash: String,
    pub err: Option<String>,
    pub message: String,
}

impl PostgresTransaction {
    pub fn new(value: &TransactionInfo, slot: Slot) -> Self {
        Self {
            signature: value.signature.to_string(),
            slot: slot as i64,
            cu_consumed: value.cu_consumed.map(|x| x as i64),
            cu_requested: value.cu_requested.map(|x| x as i64),
            prioritization_fees: value.prioritization_fees.map(|x| x as i64),
            recent_blockhash: value.recent_blockhash.to_string(),
            err: value
                .err
                .clone()
                .map(|x| BASE64.serialize(&x).ok())
                .unwrap_or(None),
            message: BinaryEncoding::Base64.encode(value.message.serialize()),
        }
    }

    pub fn to_transaction_info(&self) -> TransactionInfo {
        TransactionInfo {
            signature: Signature::from_str(self.signature.as_str()).unwrap(),
            cu_consumed: self.cu_consumed.map(|x| x as u64),
            cu_requested: self.cu_requested.map(|x| x as u32),
            prioritization_fees: self.prioritization_fees.map(|x| x as u64),
            recent_blockhash: hash_from_str(&self.recent_blockhash).expect("valid blockhash"),
            err: self
                .err
                .as_ref()
                .and_then(|x| BASE64.deserialize::<TransactionError>(x).ok()),
            message: BinaryEncoding::Base64
                .deserialize(&self.message)
                .expect("serialized message"),
            // TODO readable_accounts etc.
            readable_accounts: vec![],
            writable_accounts: vec![],
            is_vote: false,
            address_lookup_tables: vec![],
        }
    }

    pub fn build_create_table_statement(epoch: EpochRef) -> String {
        let schema = PostgresEpoch::build_schema_name(epoch);
        format!(
            r#"
                -- lookup table; maps signatures to generated int8 transaction ids
                -- no updates or deletes, only INSERTs
                CREATE TABLE {schema}.transaction_ids(
                    transaction_id bigserial PRIMARY KEY WITH (FILLFACTOR=90),
                    signature varchar(88) NOT NULL,
                    UNIQUE(signature)
                ) WITH (FILLFACTOR=100);
                -- never put sig on TOAST
                ALTER TABLE {schema}.transaction_ids ALTER COLUMN signature SET STORAGE MAIN;
                ALTER TABLE {schema}.transaction_ids
                    SET (
                        autovacuum_vacuum_scale_factor=0,
                        autovacuum_vacuum_threshold=1000,
                        autovacuum_vacuum_insert_scale_factor=0,
                        autovacuum_vacuum_insert_threshold=1000,
                        autovacuum_analyze_scale_factor=0,
                        autovacuum_analyze_threshold=1000
                        );

                -- parameter 'schema' is something like 'rpc2a_epoch_592'
                CREATE TABLE IF NOT EXISTS {schema}.transaction_blockdata(
                    -- transaction_id must exist in the transaction_ids table
                    transaction_id bigint PRIMARY KEY WITH (FILLFACTOR=90),
                    slot bigint NOT NULL,
                    cu_consumed bigint NOT NULL,
                    cu_requested bigint,
                    prioritization_fees bigint,
                    recent_blockhash text NOT NULL,
                    err text,
                    message text NOT NULL
                    -- model_transaction_blockdata
                ) WITH (FILLFACTOR=90,TOAST_TUPLE_TARGET=128);
                ALTER TABLE {schema}.transaction_blockdata ALTER COLUMN recent_blockhash SET STORAGE MAIN;
                ALTER TABLE {schema}.transaction_blockdata ALTER COLUMN message SET STORAGE EXTENDED;
                ALTER TABLE {schema}.transaction_blockdata
                    SET (
                        autovacuum_vacuum_scale_factor=0,
                        autovacuum_vacuum_threshold=1000,
                        autovacuum_vacuum_insert_scale_factor=0,
                        autovacuum_vacuum_insert_threshold=1000,
                        autovacuum_analyze_scale_factor=0,
                        autovacuum_analyze_threshold=1000
                        );
                CREATE INDEX idx_slot ON {schema}.transaction_blockdata USING btree (slot) WITH (FILLFACTOR=90);
            "#,
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

    pub async fn __create_transaction_ids(postgres_session: &PostgresSession,
                                        epoch: EpochRef,
                                        signatures: &[&str]) -> anyhow::Result<()> {
        postgres_session
            .execute(
                format!(
                    r#"
        CREATE TEMP TABLE transaction_ids_temp(
            signature varchar(88) NOT NULL
        );
        "#
                )
                    .as_str(),
                &[],
            )
            .await?;

        let statement = format!(
            r#"
            COPY transaction_ids_temp(
                signature
            ) FROM STDIN BINARY
        "#
        );
        let started_at = Instant::now();
        let sink: CopyInSink<bytes::Bytes> = postgres_session.copy_in(statement.as_str()).await?;
        let writer = BinaryCopyInWriter::new(sink, &[Type::TEXT]);
        pin_mut!(writer);
        for signature in signatures {
            writer.as_mut().write(&[&signature]).await?;
        }
        let num_rows = writer.finish().await?;
        debug!(
            "inserted {} signatures into temp table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        let statement = format!(
            r#"
        INSERT INTO {schema}.transaction_ids(signature)
        SELECT signature FROM transaction_ids_temp
        --ORDER BY signature
        ON CONFLICT(signature) DO NOTHING
        "#,
            schema = PostgresEpoch::build_schema_name(epoch),
        );
        let started_at = Instant::now();
        let num_rows = postgres_session.execute(statement.as_str(), &[]).await?;
        debug!(
            "inserted {} signatures in transactions table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

        // self.drop_temp_table(temp_table).await?;
        Ok(())
    }

    pub async fn save_transactions_from_block(
        postgres_session: &PostgresSession,
        epoch: EpochRef,
        transactions: &[PostgresTransaction],
    ) -> anyhow::Result<()> {
        let schema = PostgresEpoch::build_schema_name(epoch);

        let statement = r#"
            CREATE TEMP TABLE transaction_raw_blockdata(
                slot bigint NOT NULL,
                cu_consumed bigint NOT NULL,
                cu_requested bigint,
                prioritization_fees bigint,
                signature text NOT NULL,
                recent_blockhash text NOT NULL,
                err text,
                message text
                -- model_transaction_blockdata
            );
        "#;
        postgres_session.execute_multiple(statement).await?;

        let statement = r#"
            COPY transaction_raw_blockdata(
                slot,
                cu_consumed,
                cu_requested,
                prioritization_fees,
                signature,
                recent_blockhash,
                err,
                message
                -- model_transaction_blockdata
            ) FROM STDIN BINARY
        "#;
        let started_at = Instant::now();
        let sink: CopyInSink<bytes::Bytes> = postgres_session.copy_in(statement).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT8, // slot
                Type::INT8, // cu_consumed
                Type::INT8, // cu_requested
                Type::INT8, // prioritization_fees
                Type::TEXT, // signature
                Type::TEXT, // recent_blockhash
                Type::TEXT, // err
                Type::TEXT, // message
            ],
        );
        pin_mut!(writer);

        for tx in transactions {
            let PostgresTransaction {
                slot,
                cu_consumed,
                cu_requested,
                prioritization_fees,
                signature,
                recent_blockhash,
                err,
                message,
                // model_transaction_blockdata
            } = tx;

            writer
                .as_mut()
                .write(&[
                    &slot,
                    &cu_consumed,
                    &cu_requested,
                    &prioritization_fees,
                    &signature,
                    &recent_blockhash,
                    &err,
                    &message,
                    // model_transaction_blockdata
                ])
                .await?;
        }

        let num_rows = writer.finish().await?;
        debug!(
            "inserted {} raw transaction data rows into temp table in {:.2?}",
            num_rows,
            started_at.elapsed()
        );

        // look Ma, no clone
        // create_transaction_ids(postgres_session, epoch, &transactions.iter().map(|tx| tx.signature.as_str()).unique().collect_vec()).await?;

        let signatures = &transactions.iter().map(|tx| tx.signature.as_str()).unique().collect_vec();
        {
            postgres_session
                .execute(
                    format!(
                        r#"
        CREATE TEMP TABLE transaction_ids_temp(
            signature varchar(88) NOT NULL
        );
        "#
                    )
                        .as_str(),
                    &[],
                )
                .await?;

            let statement = format!(
                r#"
            COPY transaction_ids_temp(
                signature
            ) FROM STDIN BINARY
        "#
            );
            let started_at = Instant::now();
            let sink: CopyInSink<bytes::Bytes> = postgres_session.copy_in(statement.as_str()).await?;
            let writer = BinaryCopyInWriter::new(sink, &[Type::TEXT]);
            pin_mut!(writer);
            for signature in signatures {
                writer.as_mut().write(&[&signature]).await?;
            }
            let num_rows = writer.finish().await?;
            debug!(
            "inserted {} signatures into temp table in {}ms",
            num_rows,
            started_at.elapsed().as_millis()
        );

            let statement = format!(
                r#"
            INSERT INTO {schema}.transaction_ids(signature)
            SELECT signature FROM transaction_ids_temp
            ORDER BY signature
            ON CONFLICT(signature) DO NOTHING
            "#,
                    schema = PostgresEpoch::build_schema_name(epoch),
                );
                let started_at = Instant::now();
                let num_rows = postgres_session.execute(statement.as_str(), &[]).await?;
                debug!(
                "inserted {} signatures in transactions table in {}ms",
                num_rows,
                started_at.elapsed().as_millis()
            );

            // self.drop_temp_table(temp_table).await?;
            // Ok(())
        }

        //
        // let statement = format!(
        //     r#"
        //     INSERT INTO {schema}.transaction_ids(signature)
        //     SELECT signature from transaction_raw_blockdata
        //     ON CONFLICT DO NOTHING
        //     "#,
        // );
        // let started_at = Instant::now();
        // let num_rows = postgres_session.execute(statement.as_str(), &[]).await?;
        // debug!(
        //     "inserted {} signatures into transaction_ids table in {:.2?}",
        //     num_rows,
        //     started_at.elapsed()
        // );

        let statement = format!(
            r#"
                INSERT INTO {schema}.transaction_blockdata
                SELECT
                    ( SELECT transaction_id FROM {schema}.transaction_ids tx_lkup WHERE tx_lkup.signature = transaction_raw_blockdata.signature ),
                    slot,
                    cu_consumed,
                    cu_requested,
                    prioritization_fees,
                    recent_blockhash,
                    err,
                    message
                    -- model_transaction_blockdata
                FROM transaction_raw_blockdata
        "#,
            schema = schema,
        );
        let started_at = Instant::now();
        let num_rows = postgres_session.execute(statement.as_str(), &[]).await?;
        debug!(
            "inserted {} rows into transaction block table in {:.2?}",
            num_rows,
            started_at.elapsed()
        );

        Ok(())
    }

    pub fn build_query_statement(epoch: EpochRef, slot: Slot) -> String {
        format!(
            r#"
                SELECT
                    (SELECT signature FROM {schema}.transaction_ids tx_ids WHERE tx_ids.transaction_id = transaction_blockdata.transaction_id),
                    cu_consumed,
                    cu_requested,
                    prioritization_fees,
                    err,
                    recent_blockhash,
                    message
                    -- model_transaction_blockdata
                FROM {schema}.transaction_blockdata
                WHERE slot = {}
            "#,
            slot,
            schema = PostgresEpoch::build_schema_name(epoch),
        )
    }
}


async fn create_transaction_ids(postgres_session: &PostgresSession,
                                    epoch: EpochRef,
                                    signatures: &[&str]) -> anyhow::Result<()> {
    Ok(())
}

