use bench_lib::{config::BenchConfig, create_memo_tx_small};
use log::{info, warn};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};

// TC3 measure how much load the API endpoint can take
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    warn!("THIS IS WORK IN PROGRESS");

    let config = BenchConfig::load().unwrap();

    let rpc = Arc::new(RpcClient::new(config.lite_rpc_url.clone()));
    info!("RPC: {}", rpc.as_ref().url());

    let payer: Arc<Keypair> = Arc::new(read_keypair_file(&config.payer_path).unwrap());
    info!("Payer: {}", payer.pubkey().to_string());

    let mut txs = 0;
    let failed = Arc::new(AtomicUsize::new(0));
    let success = Arc::new(AtomicUsize::new(0));

    let hash = rpc.get_latest_blockhash().await?;
    let time_ms = config.api_load.time_ms;
    let time = tokio::time::Instant::now();

    while time.elapsed().as_millis() < time_ms.into() {
        let rpc = rpc.clone();
        let payer = payer.clone();

        let failed = failed.clone();
        let success = success.clone();

        let msg = format!("tx: {txs}");

        tokio::spawn(async move {
            let msg = msg.as_bytes();
            let tx = create_memo_tx_small(msg, &payer, hash);
            match rpc.send_transaction(&tx).await {
                Ok(_) => success.fetch_add(1, Ordering::Relaxed),
                Err(_) => failed.fetch_add(1, Ordering::Relaxed),
            };
        });

        txs += 1;
    }

    let calls_per_second = txs as f64 / (time_ms as f64 * 1000.0);
    info!("calls_per_second: {}", calls_per_second);
    info!("failed: {}", failed.load(Ordering::Relaxed));
    info!("success: {}", success.load(Ordering::Relaxed));

    Ok(())
}
