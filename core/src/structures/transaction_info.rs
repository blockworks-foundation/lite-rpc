use solana_sdk::transaction::TransactionError;

#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub signature: String,
    pub err: Option<TransactionError>,
    pub status: Result<(), TransactionError>,
    pub cu_requested: Option<u32>,
    pub prioritization_fees: Option<u64>,
    pub cu_consumed: Option<u64>,
}
