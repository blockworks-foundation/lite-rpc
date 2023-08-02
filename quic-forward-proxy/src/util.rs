#![allow(dead_code)]
use solana_sdk::signature::Keypair;
use std::env;
use std::future::Future;
use std::time::Duration;

use tokio::time::Timeout;

pub type AnyhowJoinHandle = tokio::task::JoinHandle<anyhow::Result<()>>;
pub const FALLBACK_TIMEOUT: Duration = Duration::from_secs(5);

pub fn timeout_fallback<F>(future: F) -> Timeout<F>
where
    F: Future,
{
    tokio::time::timeout(FALLBACK_TIMEOUT, future)
}

// note this is duplicated from lite-rpc module
pub async fn get_identity_keypair(identity_from_cli: &String) -> Option<Keypair> {
    if let Ok(identity_env_var) = env::var("IDENTITY") {
        if let Ok(identity_bytes) = serde_json::from_str::<Vec<u8>>(identity_env_var.as_str()) {
            Some(Keypair::from_bytes(identity_bytes.as_slice()).unwrap())
        } else {
            // must be a file
            let identity_file = tokio::fs::read_to_string(identity_env_var.as_str())
                .await
                .expect("Cannot find the identity file provided");
            let identity_bytes: Vec<u8> = serde_json::from_str(&identity_file).unwrap();
            Some(Keypair::from_bytes(identity_bytes.as_slice()).unwrap())
        }
    } else if identity_from_cli.is_empty() {
        None
    } else {
        let identity_file = tokio::fs::read_to_string(identity_from_cli.as_str())
            .await
            .expect("Cannot find the identity file provided");
        let identity_bytes: Vec<u8> = serde_json::from_str(&identity_file).unwrap();
        Some(Keypair::from_bytes(identity_bytes.as_slice()).unwrap())
    }
}
