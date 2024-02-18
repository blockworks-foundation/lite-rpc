use chrono::{DateTime, Utc};
use solana_sdk::{commitment_config::CommitmentLevel, transaction::TransactionError};


#[derive(Debug)]
pub struct TransactionNotification {
    pub signature: String,                   // 88 bytes
    pub recent_slot: u64,                    // 8 bytes
    pub forwarded_slot: u64,                 // 8 bytes
    pub forwarded_local_time: DateTime<Utc>, // 8 bytes
    pub processed_slot: Option<u64>,
    pub cu_consumed: Option<u64>,
    pub cu_requested: Option<u64>,
    pub quic_response: i16, // 8 bytes
}

#[derive(Debug)]
pub struct TransactionUpdateNotification {
    pub signature: String, // 88 bytes
    pub slot: u64,
    pub cu_consumed: Option<u64>,
    pub cu_requested: Option<u32>,
    pub cu_price: Option<u64>,
    pub transaction_status: Result<(), TransactionError>,
    pub blockhash: String,
    pub leader: String,
    pub commitment: CommitmentLevel,
}

#[derive(Debug)]
pub struct BlockNotification {
    pub slot: u64,
    pub block_leader: String,
    pub parent_slot: u64,
    pub cluster_time: DateTime<Utc>,
    pub local_time: Option<DateTime<Utc>>,
    pub blockhash: String,
    pub total_transactions: u64,
    pub block_time: u64,
    pub total_cu_consumed: u64,
    pub commitment: CommitmentLevel,
    pub transaction_found: u64,
    pub cu_consumed_by_txs: u64,
}

#[derive(Debug)]
pub struct AccountAddr {
    pub id: u32,
    pub addr: String,
}

#[derive(Debug)]
pub enum NotificationMsg {
    TxNotificationMsg(Vec<TransactionNotification>),
    BlockNotificationMsg(BlockNotification),
    AccountAddrMsg(AccountAddr),
    UpdateTransactionMsg(Vec<TransactionUpdateNotification>),
}

pub type NotificationReciever = tokio::sync::mpsc::Receiver<NotificationMsg>;
pub type NotificationSender = tokio::sync::mpsc::Sender<NotificationMsg>;
