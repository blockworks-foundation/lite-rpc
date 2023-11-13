use log::{debug, warn};
use solana_lite_rpc_core::{encoding::BASE64, structures::produced_block::TransactionInfo};
use solana_sdk::slot_history::Slot;
use tokio_postgres::types::ToSql;
use solana_lite_rpc_core::structures::epoch::{EpochRef};
use crate::postgres::postgres_epoch::PostgresEpoch;

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

const NB_ARUMENTS: usize = 8;

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
                    signature TEXT NOT NULL,
                    slot BIGINT NOT NULL,
                    err TEXT,
                    cu_requested BIGINT,
                    prioritization_fees BIGINT,
                    cu_consumed BIGINT,
                    recent_blockhash TEXT NOT NULL,
                    message TEXT NOT NULL,
                    CONSTRAINT pk_transaction_sig PRIMARY KEY(signature)
                  );
                  CREATE EXTENSION IF NOT EXISTS btree_gin;
                  CREATE INDEX idx_slot ON {schema}.transactions USING btree (slot);
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

    pub async fn save_transaction_batch(
        postgres_session: &PostgresSession,
        epoch: EpochRef,
        slot: Slot,
        transactions: &[Self],
    ) -> anyhow::Result<()> {
        let tx_count = transactions.len();
        let mut args: Vec<&(dyn ToSql + Sync)> =
            Vec::with_capacity(NB_ARUMENTS * tx_count);

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

        let values = PostgresSession::values_vecvec(NB_ARUMENTS, tx_count, &[]);
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

        debug!("Inserted {} transactions chunk into epoch schema {} for block {}", inserted, schema, slot);

        Ok(())
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
