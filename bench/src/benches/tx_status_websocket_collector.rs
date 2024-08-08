use dashmap::DashMap;
use log::debug;
use serde_json::json;
use solana_rpc_client_api::response::{Response, RpcBlockUpdate};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;
use url::Url;
use websocket_tungstenite_retry::websocket_stable;
use websocket_tungstenite_retry::websocket_stable::WsMessage;

// returns map of transaction signatures to the slot they were confirmed
// the caller must await for the token to be cancelled to prevent startup race condition
pub async fn start_tx_status_collector(
    ws_url: Url,
    payer_pubkey: Pubkey,
    commitment_config: CommitmentConfig,
    startup_token: CancellationToken,
) -> (Arc<DashMap<Signature, Slot>>, AbortHandle) {
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
    )
    .await
    .expect("Failed to connect to websocket");

    let mut channel = web_socket_slots.subscribe_message_channel();

    let observed_transactions: Arc<DashMap<Signature, Slot>> = Arc::new(DashMap::with_capacity(64));

    let observed_transactions_write = Arc::downgrade(&observed_transactions);
    let jh = tokio::spawn(async move {
        // notify the caller that we are ready to receive messages
        startup_token.cancel();
        while let Ok(msg) = channel.recv().await {
            if let WsMessage::Text(payload) = msg {
                let ws_result: jsonrpsee_types::SubscriptionResponse<Response<RpcBlockUpdate>> =
                    serde_json::from_str(&payload).unwrap();
                let block_update = ws_result.params.result;
                let slot = block_update.value.slot;
                let Some(map) = observed_transactions_write.upgrade() else {
                    debug!("observed_transactions map dropped - stopping task");
                    return;
                };
                if let Some(tx_sigs_from_block) =
                    block_update.value.block.and_then(|b| b.signatures)
                {
                    for tx_sig in tx_sigs_from_block {
                        let tx_sig = Signature::from_str(&tx_sig).unwrap();
                        debug!(
                            "Transaction signature found in block: {} - slot {}",
                            tx_sig, slot
                        );
                        map.entry(tx_sig).or_insert(slot);
                    }
                }
            }
        }
    });

    (observed_transactions, jh.abort_handle())
}
