// adapter code for all from benchrunner-service

use crate::metrics::{Metric, TxMetricData};
use crate::oldbench;
use crate::oldbench::TransactionSize;
use crate::tx_size::TxSize;
<<<<<<< Updated upstream
use log::debug;
=======
use log::{debug, error, info, warn};
>>>>>>> Stashed changes
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::fmt::Display;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
<<<<<<< Updated upstream
=======
use serde_json::{json, Value};
use solana_rpc_client_api::response::RpcBlockUpdate;
use solana_sdk::clock::Slot;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
>>>>>>> Stashed changes
use tokio::sync::RwLock;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub tx_count: usize,
    pub cu_price_micro_lamports: u64,
}

impl Display for BenchConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub async fn bench_servicerunner(
    bench_config: &BenchConfig,
    rpc_addr: String,
    funded_payer: Keypair,
    size_tx: TxSize,
) -> Metric {
    let started_at = Instant::now();

    let transaction_size = match size_tx {
        TxSize::Small => TransactionSize::Small,
        TxSize::Large => TransactionSize::Large,
    };

    debug!("Payer: {}", funded_payer.pubkey());

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        rpc_addr.clone(),
        CommitmentConfig::confirmed(),
    ));
<<<<<<< Updated upstream
    let bh = rpc_client.get_latest_blockhash().await.unwrap();
    let slot = rpc_client.get_slot().await.unwrap();
    let block_hash: Arc<RwLock<Hash>> = Arc::new(RwLock::new(bh));
    let current_slot = Arc::new(AtomicU64::new(slot));
    {
        // block hash updater task
        let block_hash = block_hash.clone();
        let rpc_client = rpc_client.clone();
        let current_slot = current_slot.clone();
        tokio::spawn(async move {
            loop {
                let bh = rpc_client.get_latest_blockhash().await;
                match bh {
                    Ok(bh) => {
                        let mut lock = block_hash.write().await;
                        *lock = bh;
                    }
                    Err(e) => println!("blockhash update error {}", e),
                }

                let slot = rpc_client.get_slot().await;
                match slot {
                    Ok(slot) => {
                        current_slot.store(slot, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(e) => println!("slot {}", e),
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
=======

    // let bh = rpc_client.get_latest_blockhash().await.unwrap();
    // let slot = rpc_client.get_slot().await.unwrap();
    let block_hash: Arc<RwLock<Hash>> = Arc::new(RwLock::new(Hash::new_unique()));
    let current_slot = Arc::new(AtomicU64::new(0 as Slot));

    {
        let block_hash = block_hash.clone();
        let rpc_client = rpc_client.clone();
        let current_slot = current_slot.clone();

        let block_subscribe =
            json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "blockSubscribe",
            "params": [
                "all",
                {
                      "commitment": "confirmed",
                      "transactionDetails": "none"
                 }
            ]
        });

        let ws_addr = rpc_addr.replace("http", "ws").replace("https", "wss");
        let mut wss = StableWebSocket::new_with_timeout(
            Url::parse(ws_addr.as_str()).unwrap(),
            block_subscribe,
            Duration::from_secs(3),
        )
            .await;

        match wss {
            Ok(mut websocket) => {
                let mut channel = websocket.subscribe_message_channel();
                start_websocket_sink(block_hash, current_slot, channel).await;
            }
            Err(_) => {
                warn!("failed to connect to websocket, falling back to polling");
                start_rpc_poller(block_hash, rpc_client.clone(), current_slot);
>>>>>>> Stashed changes
            }
        })
    };

    {
        // TODO what todo
        // not used unless log_txs is set to true
        let (tx_log_sx_null, _tx_log_rx) = tokio::sync::mpsc::unbounded_channel::<TxMetricData>();

        oldbench::bench(
            rpc_client.clone(),
            bench_config.tx_count,
            funded_payer,
            started_at.elapsed().as_micros() as u64,
            block_hash.clone(),
            current_slot.clone(),
            tx_log_sx_null,
            false, // log_transactions
            transaction_size,
            bench_config.cu_price_micro_lamports,
        )
        .await
    }
}
<<<<<<< Updated upstream
=======

async fn start_websocket_sink(block_hash: Arc<RwLock<Hash>>, current_slot: Arc<AtomicU64>, mut channel: Receiver<WsMessage>) {
    tokio::spawn(async move {
        let block_hash = block_hash.clone();
        let current_slot: Arc<AtomicU64> = current_slot.clone();
        loop {
            match channel.recv().await {
                Ok(WsMessage::Text(json_update)) => {
                    //  {"jsonrpc":"2.0","method":"blockNotification","params":{"result":{"context":{"slot":259299308},"value":{"slot":259299308,"block":{"previousBlockhash":"2Nx2ccwhxdRw8CVKvvdKycZtYTdPjG8H9zscj9GFSaLX","blockhash":"92RVdhMYTzTKBhphzYcT8yEBZdqVffAbuBwtpqgfjfK9","parentSlot":259299307,"blockTime":1710860131,"blockHeight":223228072},"err":null}},"subscription":1347293}}
                    let json_value = serde_json::from_str::<Value>(&json_update).unwrap();
                    // this is a workaround - did not find the correct dto
                    let inner_value = &json_value["params"]["result"]["value"];
                    let update: RpcBlockUpdate = serde_json::from_value(inner_value.clone()).unwrap();
                    current_slot.store(update.slot, std::sync::atomic::Ordering::Relaxed);
                    let mut lock = block_hash.write().await;
                    *lock = Hash::from_str(&update.block.unwrap().blockhash).unwrap();
                }
                Ok(WsMessage::Binary(_)) => {
                    // should not happen
                }
                Err(RecvError::Lagged(_)) => {
                    // should not happen
                }
                Err(RecvError::Closed) => {
                    error!("websocket channel for blockSubscribe closed - aborting");
                    break;
                }
            }
        }
    });
}

fn start_rpc_poller(block_hash: Arc<RwLock<Hash>>, rpc_client: Arc<RpcClient>, current_slot: Arc<AtomicU64>) {
    tokio::spawn(async move {
        loop {
            let bh = rpc_client.get_latest_blockhash().await;
            match bh {
                Ok(bh) => {
                    let mut lock = block_hash.write().await;
                    *lock = bh;
                }
                Err(e) => println!("blockhash update error {}", e),
            }

            let slot = rpc_client.get_slot().await;
            match slot {
                Ok(slot) => {
                    current_slot.store(slot, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e) => println!("slot {}", e),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
}
>>>>>>> Stashed changes
