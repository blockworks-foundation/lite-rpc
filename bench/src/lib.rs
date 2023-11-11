pub mod cli;
pub mod helpers;
pub mod metrics;
pub mod sync;
pub mod tx_logger;

// see https://spl.solana.com/memo for sizing of transactions
// As of v1.5.1, an unsigned instruction can support single-byte UTF-8 of up to 566 bytes.
// An instruction with a simple memo of 32 bytes can support up to 12 signers.
#[derive(Debug, Clone, Copy)]
pub enum TransactionSize {
    // 179 bytes, 5237 CUs
    Small,
    // 1186 bytes, 193175 CUs
    Large,
}
