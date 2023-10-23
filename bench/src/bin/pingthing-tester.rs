use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use clap::Parser;
use log::info;
use reqwest::Response;
use serde::{Deserialize, Serialize};
use solana_rpc_client::rpc_client::RpcClient;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::genesis_config::ClusterType;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use bench::pingthing::{ClusterKeys, PingThing};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(long)]
    va_api_key: String,
}


/// https://www.validators.app/ping-thing
/// see https://github.com/Block-Logic/ping-thing-client/blob/main/ping-thing-client.mjs#L161C10-L181C17
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let va_api_key = args.va_api_key;

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        "http://api.mainnet-beta.solana.com/",
        CommitmentConfig::confirmed(),
    ));
    let pingthing = PingThing {
        cluster: ClusterKeys::Mainnet,
        va_api_key,
    };

    let current_slot = rpc_client.get_slot().unwrap();
    info!("Current slot: {}", current_slot);


    // some random transaction
    let hardcoded_example = Signature::from_str("3yKgzowsEUnXXv7TdbLcHFRqrFvn4CtaNKMELzfqrvokp8Pgw4LGFVAZqvnSLp92B9oY4HGhSEZhSuYqLzkT9sC8").unwrap();

    let tx_success = true;
    let target_slot = current_slot + 0;
    info!("Target slot: {}", current_slot);


    pingthing.submit_stats(Duration::from_millis(5555), hardcoded_example, tx_success, target_slot, target_slot).await??;

    Ok(())
}


