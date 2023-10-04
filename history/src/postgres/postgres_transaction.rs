use solana_lite_rpc_core::{encoding::BASE64, structures::produced_block::TransactionInfo};

use super::postgres_session::SchemaSize;

#[derive(Debug)]
pub struct PostgresTransaction {
    pub signature: String,
    pub err: Option<String>,
    pub cu_requested: Option<i32>,
    pub prioritization_fees: Option<i64>,
    pub cu_consumed: Option<i64>,
    pub recent_blockhash: String,
    pub message: String,
}

impl SchemaSize for PostgresTransaction {
    const DEFAULT_SIZE: usize = 88 + (3 * 8) + 2;
    const MAX_SIZE: usize = Self::DEFAULT_SIZE + (3 * 8);
}

impl From<&TransactionInfo> for PostgresTransaction {
    fn from(value: &TransactionInfo) -> Self {
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
        }
    }
}
