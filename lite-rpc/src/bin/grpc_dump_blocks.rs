use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;
use clap::Parser;
use log::info;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::time::sleep;
use solana_lite_rpc_cluster_endpoints::geyser_grpc_connector::{GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig};
use solana_lite_rpc_cluster_endpoints::geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task;
use solana_lite_rpc_cluster_endpoints::grpc_store_to_disk;

#[derive(Parser)]
pub struct Args {
    /// Directory to dump blocks; must exist contain the marker file '.solana-blocks-dump'
    #[arg(long)]
    pub dump_directory: PathBuf,
    /// Seconds to run the dumper
    #[arg(long)]
    pub duration: u64,
    /// Address of the gRPC server
    #[arg(long)]
    pub grpc_addr: String,
    /// Optional token for gRPC server
    #[arg(long)]
    pub grpc_x_token: Option<String>,
}



#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let Args {
        dump_directory,
        duration: duration_seconds,
        grpc_addr,
        grpc_x_token,
    } = Args::parse();

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };
    let grpc_source_config = GrpcSourceConfig::new(grpc_addr, grpc_x_token.clone(), None, timeouts);

    let commitment_config = CommitmentConfig::confirmed();

    let (_jh_geyser_task, mut message_channel) = create_geyser_autoconnection_task(
        grpc_source_config.clone(),
        GeyserFilter(commitment_config).blocks_and_txs(),
    );

    let abort_handle = grpc_store_to_disk::spawn_block_todisk_writer(message_channel, dump_directory).await;

    // wait a bit
    info!("Run dumper for {} seconds...", duration_seconds);
    sleep(Duration::from_secs(duration_seconds)).await;
    abort_handle.abort();

    info!("Shutting down dumper.");
}
