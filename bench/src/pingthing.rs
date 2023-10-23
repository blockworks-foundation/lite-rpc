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


/// note: do not change - used on Rest Url
#[derive(Clone, Debug)]
pub enum ClusterKeys {
    Mainnet,
    Testnet,
    Devnet,
}

impl ClusterKeys {
    pub fn from_arg(cluster: String) -> Self {
        match cluster.to_lowercase().as_str() {
            "mainnet" => ClusterKeys::Mainnet,
            "testnet" => ClusterKeys::Testnet,
            "devnet" => ClusterKeys::Devnet,
            _ => panic!("incorrect cluster name"),
        }
    }
}

impl ClusterKeys {
    pub fn to_url_part(&self) -> String {
        match self {
            ClusterKeys::Mainnet => "mainnet",
            ClusterKeys::Testnet => "testnet",
            ClusterKeys::Devnet => "devnet",
        }.to_string()
    }

}

pub struct PingThing {
    // e.g. mainnet
    pub cluster: ClusterKeys,
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
            self.cluster.clone(),
            self.va_api_key.clone(),
            tx_elapsed, tx_sig, tx_success, slot_sent, slot_landed));
        jh
    }

}

// subit to https://www.validators.app/ping-thing?network=mainnet
async fn submit_stats_to_ping_thing(cluster: ClusterKeys, va_api_key: String, tx_elapsed: Duration, tx_sig: Signature, tx_success: bool, slot_sent: Slot, slot_landed: Slot)
                                    -> anyhow::Result<()> {

    let submit_data_request = Request {
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
    // cluster: 'mainnet'
    let response = client.post(format!("https://www.validators.app/api/v1/ping-thing/{}", cluster.to_url_part()))
        .header("Content-Type", "application/json")
        .header("Token", va_api_key)
        .json(&submit_data_request)
        .send()
        .await?
        .error_for_status()?;

    assert_eq!(response.status(), StatusCode::CREATED);

    debug!("Sent data for tx {} to ping-thing server", tx_sig);
    Ok(())
}


