use bench_lib::create_memo_tx_small;
use log::info;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let rpc_addr = "http://0.0.0.0:8890".to_string();
    let payer_path = "/path/to/id.json";
    let time_ms = 3000;

    let rpc = Arc::new(RpcClient::new(rpc_addr.clone()));
    info!("RPC: {}", rpc_addr);

    let payer: Arc<Keypair> = Arc::new(read_keypair_file(&payer_path).unwrap());
    info!("Payer: {}", payer.pubkey().to_string());

    let mut txs = 0;
    let failed = Arc::new(AtomicUsize::new(0));
    let success = Arc::new(AtomicUsize::new(0));

    let time = tokio::time::Instant::now();
    let hash = rpc.get_latest_blockhash().await?;

    while time.elapsed().as_millis() < time_ms {
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
    println!("calls_per_second: {}", calls_per_second);
    println!("failed: {}", failed.load(Ordering::Relaxed));
    println!("success: {}", success.load(Ordering::Relaxed));

    Ok(())
}
