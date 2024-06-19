use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use dashmap::{DashMap, DashSet};
use log::{info, trace};
use serde_json::json;
use solana_rpc_client_api::response::{Response, RpcBlockUpdate};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use tokio::task::AbortHandle;
use tokio::time::sleep;
use url::Url;
use websocket_tungstenite_retry::websocket_stable;
use websocket_tungstenite_retry::websocket_stable::WsMessage;

// TODO void race condition when this is not started up fast enough to catch transaction
// returns map of transaction signatures to the slot they were confirmed
pub async fn start_tx_status_collector(ws_url: Url, payer_pubkey: Pubkey, commitment_config: CommitmentConfig)
                                       -> (Arc<DashMap<Signature, Slot>>, AbortHandle) {
    // e.g. "commitment"
    let commitment_str = format!("{:?}", commitment_config);

    let mut web_socket_slots = websocket_stable::StableWebSocket::new_with_timeout(
        ws_url,
        json!({
          "jsonrpc": "2.0",
          "id": "1",
          "method": "blockSubscribe",
          "params": [
            {
              "mentionsAccountOrProgram": payer_pubkey.to_string(),
            },
            {
              "commitment": commitment_str,
              "encoding": "base64",
              "showRewards": false,
              "transactionDetails": "signatures"
            }
          ]
        }),
        Duration::from_secs(10),
    ).await
    .expect("Failed to connect to websocket");


    let mut channel = web_socket_slots.subscribe_message_channel();

    let observed_transactions: Arc<DashMap<Signature, Slot>> = Arc::new(DashMap::with_capacity(64));

    let observed_transactions_write = observed_transactions.clone();
    let jh = tokio::spawn(async move {
        let started_at = Instant::now();

        while let Ok(msg) = channel.recv().await {
            // TOOD use this to know when we are subscribed
            if let WsMessage::Text(payload) = msg {
                let ws_result: jsonrpsee_types::SubscriptionResponse<Response<RpcBlockUpdate>> =
                    serde_json::from_str(&payload).unwrap();
                let block_update = ws_result.params.result;
                let slot = block_update.value.slot;
                if let Some(tx_sigs_from_block) = block_update.value.block.and_then(|b| b.signatures) {
                    for tx_sig in tx_sigs_from_block {
                        let tx_sig = Signature::from_str(&tx_sig).unwrap();
                        trace!("Transaction signature: {} - slot {}", tx_sig, slot);
                        observed_transactions_write.entry(tx_sig).or_insert(slot);
                    }
                }
            }
        }
    });


    (observed_transactions, jh.abort_handle())
}