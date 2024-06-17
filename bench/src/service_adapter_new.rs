use crate::benches::confirmation_rate;
use crate::benches::confirmation_rate::send_bulk_txs_and_wait;
use crate::service_adapter1::BenchConfig;
use crate::BenchmarkTransactionParams;
use log::error;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

pub async fn benchnew_confirmation_rate_servicerunner(
    bench_config: &BenchConfig,
    rpc_addr: String,
    tx_status_websocket_addr: String,
    funded_payer: Keypair,
) -> confirmation_rate::Metric {
    let rpc = Arc::new(RpcClient::new(rpc_addr));
    let tx_params = BenchmarkTransactionParams {
        tx_size: bench_config.tx_size,
        cu_price_micro_lamports: bench_config.cu_price_micro_lamports,
    };
    let max_timeout = Duration::from_secs(60);
    let result = send_bulk_txs_and_wait(
        &rpc,
        Url::parse(&tx_status_websocket_addr).expect("Invalid URL"),
        &funded_payer,
        bench_config.tx_count,
        &tx_params,
        max_timeout,
    )
    .await;
    result.unwrap_or_else(|err| {
        error!("Failed to send bulk txs and wait: {}", err);
        confirmation_rate::Metric::default()
    })
}
