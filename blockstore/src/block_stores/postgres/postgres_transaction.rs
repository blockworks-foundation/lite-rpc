use std::error::Error;
use std::str::FromStr;
use bytes::BytesMut;

use futures_util::pin_mut;
use itertools::Itertools;
use log::debug;
use postgres_types::IsNull;
use solana_sdk::message::VersionedMessage;
use solana_sdk::pubkey::Pubkey;
use solana_lite_rpc_core::encoding::BinaryEncoding;
use solana_lite_rpc_core::solana_utils::hash_from_str;
use solana_lite_rpc_core::structures::epoch::EpochRef;
use solana_lite_rpc_core::{encoding::BASE64, structures::produced_block::TransactionInfo};
use solana_sdk::signature::Signature;
use solana_sdk::slot_history::Slot;
use solana_sdk::transaction::TransactionError;
use tokio::time::Instant;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::CopyInSink;

use super::postgres_epoch::*;
use super::postgres_session::*;

const MESSAGE_VERSION_LEGACY: i32 = -2020;
const MESSAGE_VERSION_V0: i32 = 0;

#[derive(Debug)]
pub struct PostgresTransaction {
    pub signature: String,
    pub slot: i64,
    pub idx_in_block: i32,
    pub cu_consumed: Option<i64>,
    pub cu_requested: Option<i64>,
    pub prioritization_fees: Option<i64>,
    pub recent_blockhash: String,
    pub err: Option<String>,
    // V0 -> 0, Legacy -> -2020
    pub message_version: i32,
    pub message: String,
    pub writable_accounts: Vec<String>,
    pub readable_accounts: Vec<String>,
    // note: solana uses u64 but SQL does not support that
    pub fee: i64,
    pub pre_balances: Vec<i64>,
    pub post_balances: Vec<i64>,
    pub inner_instructions: Option<Vec<String>>,
    pub log_messages: Option<Vec<String>>,
}

impl PostgresTransaction {
    pub fn new(value: &TransactionInfo, slot: Slot) -> Self {
        Self {
            signature: value.signature.to_string(),
            slot: slot as i64,
            idx_in_block: value.index,
            cu_consumed: value.cu_consumed.map(|x| x as i64),
            cu_requested: value.cu_requested.map(|x| x as i64),
            prioritization_fees: value.prioritization_fees.map(|x| x as i64),
            recent_blockhash: value.recent_blockhash.to_string(),
            err: value
                .err
                .clone()
                .map(|x| BASE64.serialize(&x).ok())
                .unwrap_or(None),
            message_version: Self::map_message_version(&value.message),
            message: BinaryEncoding::Base64.encode(value.message.serialize()),
            writable_accounts: value.writable_accounts.clone().into_iter().map(|pk| pk.to_string()).collect(),
            readable_accounts: value.readable_accounts.clone().into_iter().map(|pk| pk.to_string()).collect(),
            fee: value.fee,
            pre_balances: value.pre_balances.clone(),
            post_balances: value.post_balances.clone(),
            inner_instructions: value.inner_instructions.clone().map(|list|
                list.iter().map(|ins| BinaryEncoding::Base64.encode(bincode::serialize(&ins).unwrap())).collect_vec()
            ),
            log_messages: value.log_messages.clone(),
        }
    }

    fn map_message_version(versioned_message: &VersionedMessage) -> i32 {
        match versioned_message {
            VersionedMessage::Legacy(_) => MESSAGE_VERSION_LEGACY,
            VersionedMessage::V0(_) => MESSAGE_VERSION_V0,
        }
    }

    pub fn to_transaction_info(&self) -> TransactionInfo {
        let message = BinaryEncoding::Base64
            .deserialize(&self.message)
            .expect("serialized message");
        TransactionInfo {
            signature: Signature::from_str(self.signature.as_str()).unwrap(),
            index: self.idx_in_block,
            cu_consumed: self.cu_consumed.map(|x| x as u64),
            cu_requested: self.cu_requested.map(|x| x as u32),
            prioritization_fees: self.prioritization_fees.map(|x| x as u64),
            recent_blockhash: hash_from_str(&self.recent_blockhash).expect("valid blockhash"),
            err: self
                .err
                .as_ref()
                .and_then(|x| BASE64.deserialize::<TransactionError>(x).ok()),
            message,
            // TODO readable_accounts etc.
            readable_accounts: vec![],
            writable_accounts: vec![],
            is_vote: false,
            // TODO
            address_lookup_tables: vec![],
            fee: self.fee,
            pre_balances: self.pre_balances.clone(),
            post_balances: self.post_balances.clone(),
            inner_instructions: self.inner_instructions.clone()
                .map(|list|
                    list.iter().map(|ins| BinaryEncoding::Base64
                        .deserialize(&ins)
                        .expect("serialized inner instructions")).collect_vec()),
            log_messages: self.log_messages.clone(),
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
                    signature varchar(88) COMPRESSION lz4 NOT NULL,
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
                    idx int4 NOT NULL,
                    cu_consumed bigint NOT NULL,
                    cu_requested bigint,
                    prioritization_fees bigint,
                    recent_blockhash varchar(44) COMPRESSION lz4 NOT NULL,
                    err text COMPRESSION lz4,
                    message_version int4 NOT NULL,
                    message text COMPRESSION lz4 NOT NULL,
                    writable_accounts text[] COMPRESSION lz4,
                    readable_accounts text[] COMPRESSION lz4,
                    fee int8 NOT NULL,
                    pre_balances int8[] NOT NULL,
                    post_balances int8[] NOT NULL,
                    inner_instructions text[] COMPRESSION lz4,
                    log_messages text[] COMPRESSION lz4
                    -- model_transaction_blockdata
                ) WITH (FILLFACTOR=90,TOAST_TUPLE_TARGET=128);
                ALTER TABLE {schema}.transaction_blockdata ALTER COLUMN recent_blockhash SET STORAGE EXTENDED;
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


    pub async fn save_transactions_from_block(
        postgres_session: &PostgresSession,
        epoch: EpochRef,
        transactions: &[PostgresTransaction],
    ) -> anyhow::Result<()> {
        let schema = PostgresEpoch::build_schema_name(epoch);

        let statement = r#"
            CREATE TEMP TABLE transaction_raw_blockdata(
                slot bigint NOT NULL,
                idx int4 NOT NULL,
                cu_consumed bigint NOT NULL,
                cu_requested bigint,
                prioritization_fees bigint,
                signature varchar(88) COMPRESSION lz4 NOT NULL,
                recent_blockhash varchar(44) COMPRESSION lz4 NOT NULL,
                err text,
                message_version int4 NOT NULL,
                message text NOT NULL,
                writable_accounts text[],
                readable_accounts text[],
                fee int8 NOT NULL,
                pre_balances int8[] NOT NULL,
                post_balances int8[] NOT NULL,
                inner_instructions text[],
                log_messages text[] COMPRESSION lz4
                -- model_transaction_blockdata
            );
        "#;
        postgres_session.execute_multiple(statement).await?;

        let statement = r#"
            COPY transaction_raw_blockdata(
                slot,
                idx,
                cu_consumed,
                cu_requested,
                prioritization_fees,
                signature,
                recent_blockhash,
                err,
                message_version,
                message,
                writable_accounts,
                readable_accounts,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages
                -- model_transaction_blockdata
            ) FROM STDIN BINARY
        "#;
        let started_at = Instant::now();
        let sink: CopyInSink<bytes::Bytes> = postgres_session.copy_in(statement).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT8, // slot
                Type::INT4, // idx
                Type::INT8, // cu_consumed
                Type::INT8, // cu_requested
                Type::INT8, // prioritization_fees
                Type::VARCHAR, // signature
                Type::VARCHAR, // recent_blockhash
                Type::TEXT, // err
                Type::INT4, // message_version
                Type::TEXT, // message
                Type::TEXT_ARRAY, // writable_accounts
                Type::TEXT_ARRAY, // readable_accounts
                Type::INT8, // fee
                Type::INT8_ARRAY, // pre_balances
                Type::INT8_ARRAY, // post_balances
                Type::TEXT_ARRAY, // inner_instructions
                Type::TEXT_ARRAY, // log_messages
                // model_transaction_blockdata
            ],
        );
        pin_mut!(writer);

        for tx in transactions {
            let PostgresTransaction {
                slot,
                idx_in_block,
                cu_consumed,
                cu_requested,
                prioritization_fees,
                signature,
                recent_blockhash,
                err,
                message_version,
                message,
                writable_accounts,
                readable_accounts,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages,
                // model_transaction_blockdata
            } = tx;

            writer
                .as_mut()
                .write(&[
                    &slot,
                    &idx_in_block,
                    &cu_consumed,
                    &cu_requested,
                    &prioritization_fees,
                    &signature,
                    &recent_blockhash,
                    &err,
                    &message_version,
                    &message,
                    &writable_accounts,
                    &readable_accounts,
                    &fee,
                    &pre_balances,
                    &post_balances,
                    &inner_instructions,
                    &log_messages,
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
            "inserted {} signatures into transaction_ids table in {:.2?}",
            num_rows,
            started_at.elapsed()
        );

        let statement = format!(
            r#"
                INSERT INTO {schema}.transaction_blockdata(
                    transaction_id,
                    slot,
                    idx,
                    cu_consumed,
                    cu_requested,
                    prioritization_fees,
                    recent_blockhash,
                    err,
                    message_version,
                    message,
                    writable_accounts,
                    readable_accounts,
                    fee,
                    pre_balances,
                    post_balances,
                    inner_instructions,
                    log_messages
                    -- model_transaction_blockdata
                )
                SELECT
                    ( SELECT transaction_id FROM {schema}.transaction_ids tx_lkup WHERE tx_lkup.signature = transaction_raw_blockdata.signature ),
                    slot,
                    idx,
                    cu_consumed,
                    cu_requested,
                    prioritization_fees,
                    recent_blockhash,
                    err,
                    message_version,
                    message,
                    writable_accounts,
                    readable_accounts,
                    fee,
                    pre_balances,
                    post_balances,
                    inner_instructions,
                    log_messages
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
                    idx,
                    cu_consumed,
                    cu_requested,
                    prioritization_fees,
                    err,
                    recent_blockhash,
                    message_version,
                    message, -- TODO remove
                    --writable_accounts,
                    --readable_accounts,
                    fee,
                    pre_balances,
                    post_balances,
                    inner_instructions,
                    log_messages
                    -- model_transaction_blockdata
                FROM {schema}.transaction_blockdata
                WHERE slot = {}
            "#,
            slot,
            schema = PostgresEpoch::build_schema_name(epoch),
        )
    }
}

