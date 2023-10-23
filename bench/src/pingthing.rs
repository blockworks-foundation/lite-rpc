use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use futures::FutureExt;
use log::{debug, info};
use reqwest::{Response, StatusCode};
use serde::{Deserialize, Serialize};
use solana_rpc_client::rpc_client::RpcClient;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::genesis_config::ClusterType;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use tokio::task::JoinHandle;


pub struct PingThing {
    pub va_api_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Request {
    time: u128,
    signature: String, // Tx sig
    transaction_type: String, // 'transfer',
    success: bool, // txSuccess
    application: String, // e.g. 'web3'
    commitment_level: String, // e.g. 'confirmed'
    slot_sent: Slot,
    slot_landed: Slot,
}


impl PingThing {
    pub fn submit_stats(&self, tx_elapsed: Duration, tx_sig: Signature, tx_success: bool, slot_sent: Slot, slot_landed: Slot) -> JoinHandle<anyhow::Result<()>> {
        let jh = tokio::spawn(submit_stats_to_ping_thing(
            self.va_api_key.clone(),
            tx_elapsed, tx_sig, tx_success, slot_sent, slot_landed));
        jh
    }

}

// subit to https://www.validators.app/ping-thing?network=mainnet
async fn submit_stats_to_ping_thing(va_api_key: String, tx_elapsed: Duration, tx_sig: Signature, tx_success: bool, slot_sent: Slot, slot_landed: Slot)
    -> anyhow::Result<()> {
    // TODO only works on mainnet - skip for all others

    let foo = Request {
        time: tx_elapsed.as_millis(),
        signature: tx_sig.to_string(),
        transaction_type: "transfer".to_string(),
        success: tx_success,
        application: "LiteRPC.bench".to_string(),
        commitment_level: "confirmed".to_string(),
        slot_sent,
        slot_landed,
    };

    let client = reqwest::Client::new();
    let response = client.post("https://www.validators.app/api/v1/ping-thing/mainnet")
        .header("Content-Type", "application/json")
        .header("Token", va_api_key)
        .json(&foo)
        .send()
        .await?
        .error_for_status()?;

    assert_eq!(response.status(), StatusCode::CREATED);

    debug!("Sent data for tx {} to ping-thing server", tx_sig);
    Ok(())
}


