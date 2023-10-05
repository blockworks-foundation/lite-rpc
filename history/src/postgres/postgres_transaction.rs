use log::error;
use solana_lite_rpc_core::{encoding::BASE64, structures::produced_block::TransactionInfo};
use solana_sdk::slot_history::Slot;
use tokio_postgres::types::ToSql;

use super::postgres_session::PostgresSession;

#[derive(Debug)]
pub struct PostgresTransaction {
    pub signature: String,
    pub slot: i64,
    pub err: Option<String>,
    pub cu_requested: Option<i32>,
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
            cu_requested: value.cu_requested.map(|x| x as i32),
            prioritization_fees: value.prioritization_fees.map(|x| x as i64),
            cu_consumed: value.cu_consumed.map(|x| x as i64),
            recent_blockhash: value.recent_blockhash.clone(),
            message: value.message.clone(),
            slot: slot as i64,
        }
    }

    pub fn create_statement(schema: &String) -> String {
        format!(
            "\
        CREATE TABLE {}.TRANSACTIONS (
            signature CHAR(88) NOT NULL,
            slot BIGINT, 
            err STRING,
            cu_requested BIGINT,
            prioritization_fees BIGINT,
            cu_consumed BIGINT,
            recent_blockhash STRING NOT NULL,
            message STRING NOT NULL,
            PRIMARY KEY (signature)
            CONSTRAINT fk_transactions FOREIGN KEY (slot) REFERENCES {}.BLOCKS(slot);
          );
        ",
            schema, schema
        )
    }

    pub async fn save_transactions(
        postgres_session: &PostgresSession,
        schema: &String,
        transactions: &[Self],
    ) {
        let mut args: Vec<&(dyn ToSql + Sync)> =
            Vec::with_capacity(NB_ARUMENTS * transactions.len());

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

        let mut query = format!(
            r#"
                INSERT INTO {}.TRANSACTIONS 
                (signature, slot, err, cu_requested, prioritization_fees, cu_consumed, recent_blockhash, message)
                VALUES
            "#,
            schema
        );

        PostgresSession::multiline_query(&mut query, NB_ARUMENTS, transactions.len(), &[]);
        if let Err(e) = postgres_session.execute(&query, &args).await {
            error!(
                "Could not save transaction batch schema {}, error {}",
                schema, e
            );
        }
    }

    pub async fn get(
        postgres_session: PostgresSession,
        schema: &String,
        slot: Slot,
    ) -> Vec<TransactionInfo> {
        let statement = format!("SELECT signature, err, cu_requested, prioritization_fees, cu_consumed, recent_blockhash, message FROM {}.TRANSACTIONS WHERE SLOT = {}", schema, slot);
        let _ = postgres_session.client.query(&statement, &[]).await;
        todo!()
    }
}
