use chrono::{DateTime, Utc};
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

lazy_static::lazy_static! {
    pub static ref MESSAGES_IN_POSTGRES_CHANNEL: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_messages_in_postgres", "Number of messages in postgres")).unwrap();
    pub static ref POSTGRES_SESSION_ERRORS: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_session_errors", "Number of failures while establishing postgres session")).unwrap();
}

pub trait SchemaSize {
    const DEFAULT_SIZE: usize = 0;
    const MAX_SIZE: usize = 0;
}

#[derive(Debug)]
pub struct TransactionNotification {
    pub signature: String,                   // 88 bytes
    pub recent_slot: i64,                    // 8 bytes
    pub forwarded_slot: i64,                 // 8 bytes
    pub forwarded_local_time: DateTime<Utc>, // 8 bytes
    pub processed_slot: Option<i64>,
    pub cu_consumed: Option<i64>,
    pub cu_requested: Option<i64>,
    pub quic_response: i16, // 8 bytes
}

impl SchemaSize for TransactionNotification {
    const DEFAULT_SIZE: usize = 88 + (4 * 8);
    const MAX_SIZE: usize = Self::DEFAULT_SIZE + (3 * 8);
}

#[derive(Debug)]
pub struct TransactionUpdateNotification {
    pub signature: String,   // 88 bytes
    pub processed_slot: i64, // 8 bytes
    pub cu_consumed: Option<i64>,
    pub cu_requested: Option<i64>,
    pub cu_price: Option<i64>,
}

impl SchemaSize for TransactionUpdateNotification {
    const DEFAULT_SIZE: usize = 88 + 8;
    const MAX_SIZE: usize = Self::DEFAULT_SIZE + (3 * 8);
}

#[derive(Debug)]
pub struct BlockNotification {
    pub slot: i64,                   // 8 bytes
    pub leader_id: i64,              // 8 bytes
    pub parent_slot: i64,            // 8 bytes
    pub cluster_time: DateTime<Utc>, // 8 bytes
    pub local_time: Option<DateTime<Utc>>,
}

impl SchemaSize for BlockNotification {
    const DEFAULT_SIZE: usize = 4 * 8;
    const MAX_SIZE: usize = Self::DEFAULT_SIZE + 8;
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

pub type NotificationReciever = UnboundedReceiver<NotificationMsg>;
pub type NotificationSender = UnboundedSender<NotificationMsg>;
