use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use solana_rpc_client::nonblocking::rpc_client::RpcClient;

use crate::{cli::RpcArgs, helpers::BenchHelper};

use super::Strategy;

#[derive(Debug, serde::Serialize)]
pub struct Tc3Result {
    calls_per_second: f64,
    failed: usize,
    success: usize,
}

/// measure how much load the API endpoint can take (send_transaction calls per second)
#[derive(clap::Args, Debug)]
pub struct Tc3 {
    #[command(flatten)]
    rpc_cli_options: RpcArgs,
    #[arg(short = 't', long, default_value_t = 3000)]
    time_ms: u128,
}

#[async_trait::async_trait]
impl Strategy for Tc3 {
    type Output = Tc3Result;

    async fn execute(&self) -> anyhow::Result<Self::Output> {
        let rpc = Arc::new(RpcClient::new(self.rpc_cli_options.rpc_addr.clone()));
        let payer = Arc::new(BenchHelper::get_payer(&self.rpc_cli_options.payer).await?);

        let mut txs = 0;
        let failed = Arc::new(AtomicUsize::new(0));
        let success = Arc::new(AtomicUsize::new(0));

        let time = tokio::time::Instant::now();
        let hash = rpc.get_latest_blockhash().await?;

        while time.elapsed().as_millis() < self.time_ms {
            let rpc = rpc.clone();
            let payer = payer.clone();

            let failed = failed.clone();
            let success = success.clone();

            let msg = format!("tx: {txs}");

            tokio::spawn(async move {
                let msg = msg.as_bytes();

                let tx = BenchHelper::create_memo_tx_small(msg, &payer, hash);

                match rpc.send_transaction(&tx).await {
                    Ok(_) => success.fetch_add(1, Ordering::Relaxed),
                    Err(_) => failed.fetch_add(1, Ordering::Relaxed),
                };
            });

            txs += 1;
        }

        let calls_per_second = txs as f64 / (self.time_ms as f64 * 1000.0);

        Ok(Tc3Result {
            calls_per_second,
            failed: failed.load(Ordering::Relaxed),
            success: success.load(Ordering::Relaxed),
        })
    }
}
