use const_env::from_env;
use solana_transaction_status::TransactionConfirmationStatus;

pub mod bridge;
pub mod bridge_pubsub;
pub mod cli;
pub mod configs;
pub mod errors;
pub mod jsonrpsee_subscrption_handler_sink;
pub mod postgres_logger;
pub mod rpc;
pub mod rpc_errors;
pub mod rpc_pubsub;
pub mod service_spawner;
pub mod start_server;
mod rpc_custom_errors;

#[from_env]
pub const DEFAULT_RPC_ADDR: &str = "http://0.0.0.0:8899";
#[from_env]
pub const DEFAULT_WS_ADDR: &str = "ws://0.0.0.0:8900";

#[from_env]
pub const DEFAULT_MAX_NUMBER_OF_TXS_IN_QUEUE: usize = 200_000;

/// 25 slots in 10s send to little more leaders
#[from_env]
pub const DEFAULT_FANOUT_SIZE: u64 = 18;

#[from_env]
pub const MAX_RETRIES: usize = 40;

pub const DEFAULT_RETRY_TIMEOUT: u64 = 3;

#[from_env]
pub const DEFAULT_CLEAN_INTERVAL_MS: u64 = 5 * 60 * 1000; // five minute
pub const DEFAULT_TRANSACTION_CONFIRMATION_STATUS: TransactionConfirmationStatus =
    TransactionConfirmationStatus::Finalized;

#[from_env]
pub const DEFAULT_GRPC_ADDR: &str = "http://localhost:10000";

#[from_env]
pub const GRPC_VERSION: &str = "1.16.1";

// cache transactions of 1000 slots by default
#[from_env]
pub const NB_SLOTS_TRANSACTIONS_TO_CACHE: u64 = 1000;

#[from_env]
pub const MAX_NB_OF_CONNECTIONS_WITH_LEADERS: usize = 8;

#[from_env]
pub const ENABLE_ADDRESS_LOOKUP_TABLES: bool = false;
