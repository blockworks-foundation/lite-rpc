use std::sync::Arc;
use std::time::Duration;
use log::error;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Keypair;
use tokio::time::Instant;
use crate::benches::confirmation_rate;
use crate::benches::confirmation_rate::{send_bulk_txs_and_wait};
use crate::BenchmarkTransactionParams;
use crate::service_adapter1::BenchConfig;
use crate::tx_size::TxSize;

pub async fn benchnew_confirmation_rate_servicerunner(
    bench_config: &BenchConfig,
    rpc_addr: String,
    funded_payer: Keypair,
) -> confirmation_rate::Metric {
    let rpc = Arc::new(RpcClient::new(rpc_addr));
    let tx_params = BenchmarkTransactionParams {
        tx_size: TxSize::Small,
        cu_price_micro_lamports: bench_config.cu_price_micro_lamports,
    };
    let max_timeout = Duration::from_secs(60);
    let result = send_bulk_txs_and_wait(&rpc, &funded_payer, bench_config.tx_count, &tx_params, max_timeout).await;
    result.unwrap_or_else(|err| {
        error!("Failed to send bulk txs and wait: {}", err);
        confirmation_rate::Metric::default()
    })
}
