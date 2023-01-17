use const_env::from_env;
use solana_transaction_status::TransactionConfirmationStatus;

pub mod bridge;
pub mod cli;
pub mod configs;
pub mod encoding;
pub mod errors;
pub mod rpc;
pub mod workers;

#[from_env]
pub const DEFAULT_RPC_ADDR: &str = "http://[::]:8899";
#[from_env]
pub const DEFAULT_LITE_RPC_ADDR: &str = "http://[::]:8890";
#[from_env]
pub const DEFAULT_WS_ADDR: &str = "ws://[::]:8900";
#[from_env]
pub const DEFAULT_TX_MAX_RETRIES: u16 = 1;
#[from_env]
pub const DEFAULT_TX_BATCH_SIZE: usize = 1 << 7;
#[from_env]
pub const DEFAULT_FANOUT_SIZE: u64 = 32;
#[from_env]
pub const DEFAULT_TX_BATCH_INTERVAL_MS: u64 = 1;
#[from_env]
pub const DEFAULT_CLEAN_INTERVAL_MS: u64 = 5 * 60 * 1000; // five minute
#[from_env]
pub const DEFAULT_TX_SENT_TTL_S: u64 = 12;
pub const DEFAULT_TRANSACTION_CONFIRMATION_STATUS: TransactionConfirmationStatus =
    TransactionConfirmationStatus::Finalized;
