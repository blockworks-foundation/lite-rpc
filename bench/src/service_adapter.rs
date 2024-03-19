// adapter code for all from benchrunner-service

use crate::metrics::{Metric, TxMetricData};
use crate::oldbench;
use crate::oldbench::TransactionSize;
use crate::tx_size::TxSize;
use log::{debug, error, info};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::hash::Hash;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use serde_json::{json, Value};
use solana_rpc_client_api::response::RpcBlockUpdate;
use solana_sdk::clock::Slot;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::RwLock;
use tokio::time::Instant;
use websocket_tungstenite_retry::websocket_stable::{StableWebSocket, WsMessage};
use url::Url;

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

    // let bh = rpc_client.get_latest_blockhash().await.unwrap();
    // let slot = rpc_client.get_slot().await.unwrap();
    let block_hash: Arc<RwLock<Hash>> = Arc::new(RwLock::new(Hash::new_unique()));
    let current_slot = Arc::new(AtomicU64::new(0 as Slot));
    {
        let slot_subscribe =
            json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "slotSubscribe",
        });

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
        info!("Connecting to {}", ws_addr);
        let mut wss = StableWebSocket::new_with_timeout(
            Url::parse(ws_addr.as_str()).unwrap(),
            block_subscribe,
            Duration::from_secs(3),
        )
            .await
            .unwrap();

        let mut channel = wss.subscribe_message_channel();
        loop {
            match channel.recv().await {
                Ok(WsMessage::Text(json_update)) => {
                    let update: RpcBlockUpdate = serde_json::from_str(&json_update).unwrap();
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