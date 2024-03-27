use solana_sdk::signature::Keypair;
use tokio::time::Instant;
use crate::metrics::Metric;
use crate::service_adapter1::BenchConfig;
use crate::tx_size::TxSize;

pub async fn benchnew_confirmation_rate_servicerunner(
    bench_config: &BenchConfig,
    rpc_addr: String,
    funded_payer: Keypair,
    size_tx: TxSize,
) -> Metric {
    let started_at = Instant::now();



    todo!()
}
