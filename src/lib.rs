use const_env::from_env;
use solana_transaction_status::TransactionConfirmationStatus;

pub mod bridge;
pub mod cli;
pub mod configs;
pub mod encoding;
pub mod errors;
pub mod rpc;
pub mod workers;

pub type WireTransaction = Vec<u8>;

#[from_env]
pub const DEFAULT_RPC_ADDR: &str = "http://127.0.0.1:8899";
#[from_env]
pub const DEFAULT_LITE_RPC_ADDR: &str = "http://127.0.0.1:8890";
#[from_env]
pub const DEFAULT_WS_ADDR: &str = "ws://127.0.0.1:8900";
#[from_env]
pub const DEFAULT_TX_MAX_RETRIES: u16 = 1;
#[from_env]
pub const TX_MAX_RETRIES_UPPER_LIMIT: u16 = 5;
#[from_env]
pub const DEFAULT_TX_RETRY_BATCH_SIZE: usize = 20;
pub const DEFAULT_TRANSACTION_CONFIRMATION_STATUS: TransactionConfirmationStatus =
    TransactionConfirmationStatus::Finalized;
