use bench::pingthing::{ClusterKeys, PingThing};
use clap::Parser;
use log::info;
use solana_rpc_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

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
    let hardcoded_example = Signature::from_str(
        "3yKgzowsEUnXXv7TdbLcHFRqrFvn4CtaNKMELzfqrvokp8Pgw4LGFVAZqvnSLp92B9oY4HGhSEZhSuYqLzkT9sC8",
    )
    .unwrap();

    let tx_success = true;

    pingthing
        .submit_stats(
            Duration::from_millis(5555),
            hardcoded_example,
            tx_success,
            current_slot,
            current_slot,
        )
        .await??;

    Ok(())
}
