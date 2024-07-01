use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use bimap::BiMap;


use crate::block_stores::postgres::{BlockstorePostgresSessionConfig, json_deserialize, json_serialize};
use futures_util::pin_mut;
use itertools::Itertools;
use log::{debug, info, warn};
use serde_json::Value;
use solana_lite_rpc_core::encoding::BinaryEncoding;
use solana_lite_rpc_core::solana_utils::hash_from_str;
use solana_lite_rpc_core::structures::epoch::{EpochRef};
use solana_lite_rpc_core::{structures::produced_block::TransactionInfo};
use solana_sdk::message::VersionedMessage;
use solana_sdk::signature::Signature;
use solana_sdk::slot_history::Slot;
use solana_sdk::transaction::TransactionError;
use solana_transaction_status::UiTransactionTokenBalance;
use tokio::time::Instant;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{Type};
use tokio_postgres::CopyInSink;
use tracing::trace_span;
use tracing_subscriber::EnvFilter;
use crate::block_stores::postgres::postgres_block::PostgresBlock;
use crate::block_stores::postgres::postgres_block_store_query::PostgresQueryBlockStore;
use crate::block_stores::postgres::postgres_block_store_writer::PostgresBlockStore;

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
    pub err: Option<Value>,
    // V0 -> 0, Legacy -> -2020
    pub message_version: i32,
    pub message: String,
    pub writable_accounts: Vec<String>,
    pub readable_accounts: Vec<String>,
    // note: solana uses u64 but SQL does not support that
    pub fee: i64,
    pub pre_balances: Vec<i64>,
    pub post_balances: Vec<i64>,
    pub inner_instructions: Option<Vec<Value>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Vec<Value>,
    pub post_token_balances: Vec<Value>,
}

impl PostgresTransaction {
    pub fn new(value: &TransactionInfo, slot: Slot) -> Self {
        let _span = trace_span!("PostgresTransaction::new", ?slot).entered();
        Self {
            signature: value.signature.to_string(),
            slot: slot as i64,
            idx_in_block: value.index,
            cu_consumed: value.cu_consumed.map(|x| x as i64),
            cu_requested: value.cu_requested.map(|x| x as i64),
            prioritization_fees: value.prioritization_fees.map(|x| x as i64),
            recent_blockhash: value.recent_blockhash.to_string(),
            err: value.err.clone().map(|x| json_serialize(&x)),
            message_version: Self::map_message_version(&value.message),
            message: BinaryEncoding::Base64.encode(value.message.serialize()),
            writable_accounts: value
                .writable_accounts
                .clone()
                .into_iter()
                .map(|pk| pk.to_string())
                .collect(),
            readable_accounts: value
                .readable_accounts
                .clone()
                .into_iter()
                .map(|pk| pk.to_string())
                .collect(),
            fee: value.fee,
            pre_balances: value.pre_balances.clone(),
            post_balances: value.post_balances.clone(),
            inner_instructions: value
                .inner_instructions
                .clone()
                .map(|list| list.iter().map(|ins| json_serialize(&ins)).collect_vec()),
            log_messages: value.log_messages.clone(),
            pre_token_balances: value
                .pre_token_balances
                .iter()
                .map(|x| json_serialize(x))
                .collect(),
            post_token_balances: value
                .post_token_balances
                .iter()
                .map(|x| json_serialize(x))
                .collect(),
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
                .clone()
                .map(|x| json_deserialize::<TransactionError>(x)),
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
            inner_instructions: self.inner_instructions.clone().map(|list| {
                list.into_iter()
                    .map(|ins| json_deserialize(ins))
                    .collect_vec()
            }),
            log_messages: self.log_messages.clone(),
            pre_token_balances: self
                .pre_token_balances
                .clone()
                .into_iter()
                .map(|x| json_deserialize::<UiTransactionTokenBalance>(x))
                .collect(),
            post_token_balances: self
                .post_token_balances
                .clone()
                .into_iter()
                .map(|x| json_deserialize::<UiTransactionTokenBalance>(x))
                .collect(),
        }
    }

    pub fn build_create_table_statement(epoch: EpochRef) -> String {
        let schema = PostgresEpoch::build_schema_name(epoch);
        format!(
            r#"
                -- parameter 'schema' is something like 'rpc2a_epoch_592'
                CREATE TABLE IF NOT EXISTS {schema}.transaction_blockdata(
                    slot bigint NOT NULL,
                     -- transaction_id must exist in the transaction_ids table
                    transaction_id bigint NOT NULL,
                    -- signature varchar(88) NOT NULL,
                    idx int4 NOT NULL,
                    cu_consumed bigint NOT NULL,
                    cu_requested bigint,
                    prioritization_fees bigint,
                    recent_blockhash varchar(44) COMPRESSION lz4 NOT NULL,
                    err jsonb COMPRESSION lz4,
                    message_version int4 NOT NULL,
                    message text COMPRESSION lz4 NOT NULL,
                    writable_accounts text[] COMPRESSION lz4,
                    readable_accounts text[] COMPRESSION lz4,
                    fee int8 NOT NULL,
                    pre_balances int8[] NOT NULL,
                    post_balances int8[] NOT NULL,
                    inner_instructions jsonb[] COMPRESSION lz4,
                    log_messages text[] COMPRESSION lz4,
                    pre_token_balances jsonb[] NOT NULL,
                    post_token_balances jsonb[] NOT NULL,
                    -- model_transaction_blockdata
                    PRIMARY KEY (slot, transaction_id) WITH (FILLFACTOR=90)
                ) WITH (FILLFACTOR=90,TOAST_TUPLE_TARGET=128);
                --ALTER TABLE {schema}.transaction_blockdata ALTER COLUMN signature SET STORAGE PLAIN;
                ALTER TABLE {schema}.transaction_blockdata ALTER COLUMN recent_blockhash SET STORAGE EXTENDED;
                ALTER TABLE {schema}.transaction_blockdata ALTER COLUMN message SET STORAGE EXTENDED;
                ALTER TABLE {schema}.transaction_blockdata
                    SET (
                        autovacuum_vacuum_scale_factor=0.2,
                        autovacuum_vacuum_threshold=10000,
                        autovacuum_vacuum_insert_scale_factor=0.2,
                        autovacuum_vacuum_insert_threshold=10000,
                        autovacuum_analyze_scale_factor=0.2,
                        autovacuum_analyze_threshold=10000
                        );
                CREATE INDEX idx_slot ON {schema}.transaction_blockdata USING btree (slot) WITH (FILLFACTOR=90);
            "#,
            schema = schema
        )
    }

    pub fn build_citus_distribute_table_statement(epoch: EpochRef, table_name: &str, distribution_column: &str) -> String {
        let schema = PostgresEpoch::build_schema_name(epoch);
        format!(
            r#"
                DO $$
                    DECLARE has_citus boolean;
                BEGIN
                    has_citus = exists (SELECT name FROM pg_available_extensions() WHERE name='citus');
                    IF has_citus THEN
                        RAISE INFO 'Use citus extension, will distribute table';
                        PERFORM create_distributed_table('{schema}.{table_name}', '{distribution_column}');
                    ELSE
                        RAISE INFO 'No citus extension found';
                    END IF;
                END; $$
            "#,
            schema = schema,
            table_name = table_name,
            distribution_column = distribution_column,
        )
    }

    // assumes that sigature mappings are available
    pub async fn save_transactions_from_block(
        postgres_session: &PostgresSession,
        epoch: EpochRef,
        tx_mapping: Arc<BiMap<String, i64>>,
        transactions: &[PostgresTransaction],
    ) -> anyhow::Result<()> {
        let schema = PostgresEpoch::build_schema_name(epoch);

        let started_at = Instant::now();

        // let statement = r#"
        //     CREATE TEMP TABLE transaction_raw_blockdata(
        //         slot bigint NOT NULL,
        //         transaction_id bigint NOT NULL,
        //         idx int4 NOT NULL,
        //         cu_consumed bigint NOT NULL,
        //         cu_requested bigint,
        //         prioritization_fees bigint,
        //         recent_blockhash varchar(44) COMPRESSION lz4 NOT NULL,
        //         err jsonb,
        //         message_version int4 NOT NULL,
        //         message text NOT NULL,
        //         writable_accounts text[],
        //         readable_accounts text[],
        //         fee int8 NOT NULL,
        //         pre_balances int8[] NOT NULL,
        //         post_balances int8[] NOT NULL,
        //         inner_instructions jsonb[],
        //         log_messages text[] COMPRESSION lz4,
        //         pre_token_balances jsonb[] NOT NULL,
        //         post_token_balances jsonb[] NOT NULL
        //         -- model_transaction_blockdata
        //     );
        // "#;
        // postgres_session.execute_multiple(statement).await?;

        let statement = format!(r#"
            COPY {schema}.transaction_blockdata(
                slot,
                transaction_id,
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
                log_messages,
                pre_token_balances,
                post_token_balances
                -- model_transaction_blockdata
            ) FROM STDIN BINARY
        "#, schema = schema);
        let started_at = Instant::now();
        let sink: CopyInSink<bytes::Bytes> = postgres_session.copy_in(&statement).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::INT8,        // slot
                Type::INT8,        // transaction_id
                Type::INT4,        // idx
                Type::INT8,        // cu_consumed
                Type::INT8,        // cu_requested
                Type::INT8,        // prioritization_fees
                Type::VARCHAR,     // recent_blockhash
                Type::JSONB,       // err
                Type::INT4,        // message_version
                Type::TEXT,        // message
                Type::TEXT_ARRAY,  // writable_accounts
                Type::TEXT_ARRAY,  // readable_accounts
                Type::INT8,        // fee
                Type::INT8_ARRAY,  // pre_balances
                Type::INT8_ARRAY,  // post_balances
                Type::JSONB_ARRAY, // inner_instructions
                Type::TEXT_ARRAY,  // log_messages
                Type::JSONB_ARRAY, // pre_token_balances
                Type::JSONB_ARRAY, // post_token_balances
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
                pre_token_balances,
                post_token_balances,
                // model_transaction_blockdata
            } = tx;

            let transaction_id = tx_mapping.get_by_left(signature).expect("transaction_id must exist in mapping table");

            writer
                .as_mut()
                .write(&[
                    &slot,
                    &transaction_id,
                    &idx_in_block,
                    &cu_consumed,
                    &cu_requested,
                    &prioritization_fees,
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
                    &pre_token_balances,
                    &post_token_balances,
                    // model_transaction_blockdata
                ])
                .await?;
        }

        let num_rows = writer.finish().await?;
        debug!(
            "inserted {} raw transaction data rows into temp table in {:.2}ms",
            num_rows,
            started_at.elapsed().as_secs_f64() * 1000.0
        );

        // let statement = format!(
        //     r#"
        //         INSERT INTO {schema}.transaction_blockdata(
        //             slot,
        //             transaction_id,
        //             signature,
        //             idx,
        //             cu_consumed,
        //             cu_requested,
        //             prioritization_fees,
        //             recent_blockhash,
        //             err,
        //             message_version,
        //             message,
        //             writable_accounts,
        //             readable_accounts,
        //             fee,
        //             pre_balances,
        //             post_balances,
        //             inner_instructions,
        //             log_messages,
        //             pre_token_balances,
        //             post_token_balances
        //             -- model_transaction_blockdata
        //         )
        //         SELECT
        //             slot,
        //             transaction_id,
        //             signature,
        //             idx,
        //             cu_consumed,
        //             cu_requested,
        //             prioritization_fees,
        //             recent_blockhash,
        //             err,
        //             message_version,
        //             message,
        //             writable_accounts,
        //             readable_accounts,
        //             fee,
        //             pre_balances,
        //             post_balances,
        //             inner_instructions,
        //             log_messages,
        //             pre_token_balances,
        //             post_token_balances
        //             -- model_transaction_blockdata
        //         FROM transaction_raw_blockdata
        //         INNER JOIN transaction_ids_temp_mapping USING(signature)
        // "#,
        //     schema = schema,
        // );
        // let started_at = Instant::now();
        //
        // // postgres_session.execute_explain(&statement, &[], Duration::from_millis(50)).await?;
        // postgres_session.execute(&statement, &[]).await?;

        debug!(
            "inserted {} rows into transaction block table in {:.2}ms",
            num_rows,
            started_at.elapsed().as_secs_f64() * 1000.0
        );

        Ok(())
    }

    pub fn build_query_statement(epoch: EpochRef, slot: Slot) -> String {
        // (SELECT signature FROM {schema}.transaction_ids tx_ids WHERE tx_ids.transaction_id = transaction_blockdata.transaction_id),
        format!(
            r#"
                SELECT
                    signature,
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
                    log_messages,
                    pre_token_balances,
                    post_token_balances
                    -- model_transaction_blockdata
                FROM {schema}.transaction_blockdata
                INNER JOIN {schema}.transaction_ids tx_ids USING(transaction_id)
                WHERE slot = {}
            "#,
            slot,
            schema = PostgresEpoch::build_schema_name(epoch),
        )
    }
}

#[tokio::test]
async fn write_speed() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_str("info,solana_lite_rpc_blockstore::block_stores::postgres::postgres_transaction=debug").unwrap())
        .init();

    // BENCH_PG_CONFIG=host=localhost dbname=literpc3 user=literpc_app password=litelitesecret sslmode=disable
    let pg_session_config = BlockstorePostgresSessionConfig::new_from_env("BENCH").unwrap();
    let epoch = EpochRef::new(334);

    let session = PostgresSession::new_writer(pg_session_config.clone()).await.unwrap();

    let now = SystemTime::now();
    let start_slot_value = now.duration_since(UNIX_EPOCH).unwrap().as_secs() - 1710403000;


    // let create_schema = PostgresEpoch::build_create_schema_statement(epoch);
    // let create_block = PostgresBlock::build_create_table_statement(epoch);
    // let create_transactin = PostgresTransaction::build_create_table_statement(epoch);
    // session.execute_multiple(&create_schema).await.unwrap();
    // session.execute_multiple(&create_block).await.unwrap();
    // session.execute_multiple(&create_transactin).await.unwrap();

    for run in 0..100 {
        session.clear_session().await;
        info!("-----------------------------------------");
        info!("starting run {}", run);
        let transactions = (0..1000).map(|idx| create_tx((start_slot_value + run) as i64, idx)).collect_vec();
        // let started_at = Instant::now();
        // PostgresTransaction::save_transactions_from_block(&session, epoch, &transactions).await.expect("save must succeed");

        let statement = format!(
            r#"
            CREATE TEMP TABLE transaction_ids_temp_mapping AS WITH mapping AS (
                INSERT INTO {schema}.transaction_ids(signature)
                SELECT signature FROM {schema}.example_signatures
                -- overlay(signature placing 'aaa' from 2 for 4)
                ON CONFLICT DO NOTHING
                RETURNING *
            )
            SELECT transaction_id, signature FROM mapping
            "#,
            schema = "rpc2a_epoch_610"
        );
        let started_at = Instant::now();
        session.execute(statement.as_str(), &[]).await.unwrap();
        info!("inserted {} signatures into transaction_ids table in {:.2}ms", transactions.len(), started_at.elapsed().as_secs_f64() * 1000.0);


        info!(".. done with run {}", run);
    }

}

fn create_tx(slot: i64, idx_in_block: i32) -> PostgresTransaction {
    let signature = Signature::new_unique().to_string();
    PostgresTransaction {
        signature,
        slot,
        idx_in_block,
        cu_consumed: Some(1),
        cu_requested: Some(1),
        prioritization_fees: Some(1),
        recent_blockhash: "recent_blockhash".to_string(),
        err: Some(Value::Null),
        message_version: 1,
        message: "message".to_string(),
        writable_accounts: vec!["writable_accounts".to_string()],
        readable_accounts: vec!["readable_accounts".to_string()],
        fee: 1,
        pre_balances: vec![1],
        post_balances: vec![1],
        inner_instructions: Some(vec![Value::Null]),
        log_messages: Some(vec!["log_messages".to_string()]),
        pre_token_balances: vec![Value::Null],
        post_token_balances: vec![Value::Null],
    }
}
