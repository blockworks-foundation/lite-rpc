
use log::{error, info, warn};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::fs::create_dir;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use solana_rpc_client::nonblocking::rpc_client::RpcClient;

use tracing::debug;


use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;
use yellowstone_grpc_proto::prost::Message;
use solana_lite_rpc_blockstore::block_stores::postgres::BlockstorePostgresSessionConfig;
use solana_lite_rpc_blockstore::block_stores::postgres::postgres_block_store_writer::PostgresBlockStore;
use solana_lite_rpc_cluster_endpoints::grpc_subscription::from_grpc_block_update;
use solana_lite_rpc_core::structures::epoch::EpochCache;

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let blockfiles_index_file = std::env::var("BLOCKFILES_INDEX").expect("env var BLOCKFILES_INDEX is mandatory");
    let blockfiles_index_file = PathBuf::from_str(&blockfiles_index_file).expect("must be filename");
    let pg_config = BlockstorePostgresSessionConfig::new_from_env("BENCH").unwrap();

    let rpc_url = std::env::var("RPC_URL").expect("env var RPC_URL is mandatory");
    let rpc_client = Arc::new(RpcClient::new(rpc_url));

    let (epoch_cache, _) = EpochCache::bootstrap_epoch(&rpc_client).await.unwrap();

    let block_store = PostgresBlockStore::new(epoch_cache, pg_config).await;


    let file = File::open(blockfiles_index_file).unwrap();
    let lines = io::BufReader::new(file).lines();
    for filename in lines {
        let filename = filename.unwrap();
        let block_file = PathBuf::from_str(&filename).unwrap();
        info!("filename: {:?}", filename);

        let (slot_from_file, epoch_ms) =
            parse_slot_and_timestamp_from_file(block_file.file_name().unwrap().to_str().unwrap());
        info!("slot from file: {}", slot_from_file);
        info!("epochms from file: {}", epoch_ms);

        let block_bytes = std::fs::read(block_file).unwrap();
        debug!("read {} bytes from block file", block_bytes.len());
        let decoded = SubscribeUpdateBlock::decode(block_bytes.as_slice()).expect("Block file must be protobuf");

        info!("decoded block: {}", decoded.slot);

        let produced_block = from_grpc_block_update(decoded, CommitmentConfig::confirmed());

        block_store.prepare_epoch_schema(produced_block.slot).await.expect("prepare epoch schema must succeed");
        let result = block_store.save_confirmed_block(&produced_block).await;

        match result {
            Ok(()) => {
                info!("block {} saved", produced_block.slot);
            }
            Err(err) => {
                error!("block {} not saved: {}", produced_block.slot, err);
            }
        }
    }

}


// e.g. block-000251395041-confirmed-1707312285514.dat
#[allow(dead_code)]
pub fn parse_slot_and_timestamp_from_file(file_name: &str) -> (Slot, u64) {
    let slot_str = file_name.split('-').nth(1).unwrap();
    let slot = slot_str.parse::<Slot>().unwrap();
    let timestamp_str = file_name
        .split('-')
        .nth(3)
        .unwrap()
        .split('.')
        .next()
        .unwrap();
    info!("parsed slot {} from file name", slot);
    let epoch_ms = timestamp_str.parse::<u64>().unwrap();
    (slot, epoch_ms)
}

