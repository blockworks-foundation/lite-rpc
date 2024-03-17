
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
use tokio::time::Instant;

use tracing::debug;


use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;
use yellowstone_grpc_proto::prost::Message;
use solana_lite_rpc_blockstore::block_stores::postgres::BlockstorePostgresSessionConfig;
use solana_lite_rpc_blockstore::block_stores::postgres::postgres_block_store_writer::PostgresBlockStore;
use solana_lite_rpc_cluster_endpoints::grpc_subscription::from_grpc_block_update;
use solana_lite_rpc_core::structures::epoch::EpochCache;
use solana_lite_rpc_util::histogram_nbuckets::histogram;

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let blockfiles_index_file = std::env::var("BLOCKS_LIST_FILE").expect("env var BLOCKS_LIST_FILE is mandatory");
    let blockfiles_index_file = PathBuf::from_str(&blockfiles_index_file).expect("must be filename");
    let pg_config = BlockstorePostgresSessionConfig::new_from_env("BENCH").unwrap();

    let rpc_url = std::env::var("RPC_URL").expect("env var RPC_URL is mandatory");
    let rpc_client = Arc::new(RpcClient::new(rpc_url));

    let (epoch_cache, _) = EpochCache::bootstrap_epoch(&rpc_client).await.unwrap();

    let block_store = PostgresBlockStore::new(epoch_cache, pg_config).await;


    let mut total_save_times: Vec<f64> = Vec::with_capacity(1000);
    let mut block_save_times: Vec<f64> = Vec::with_capacity(1000);
    let mut transactions_save_times: Vec<f64> = Vec::with_capacity(1000);
    let file = File::open(blockfiles_index_file).unwrap();
    let lines = io::BufReader::new(file).lines();
    for filename in lines {
        let filename = filename.unwrap();
        let block_file = PathBuf::from_str(&filename).unwrap();

        let (_slot_from_file, _epoch_ms) =
            parse_slot_and_timestamp_from_file(block_file.file_name().unwrap().to_str().unwrap());

        let block_bytes = std::fs::read(block_file).unwrap();
        debug!("read {} bytes from block file", block_bytes.len());
        let decoded = SubscribeUpdateBlock::decode(block_bytes.as_slice()).expect("Block file must be protobuf");

        let produced_block = from_grpc_block_update(decoded, CommitmentConfig::confirmed());

        block_store.prepare_epoch_schema(produced_block.slot).await.expect("prepare epoch schema must succeed");
        let started_at = Instant::now();
        let result = block_store.save_confirmed_block(&produced_block).await;
        let elapsed = started_at.elapsed();

        match result {
            Ok((elapsed_block, elapsed_txs)) => {
                info!("block {} saved in {} ms ({:?} + {:?})",
                    produced_block.slot, elapsed.as_secs_f64() * 1000.0, elapsed_block, elapsed_txs);
                total_save_times.push(elapsed.as_secs_f64() * 1000.0);
                block_save_times.push(elapsed_block.as_secs_f64() * 1000.0);
                transactions_save_times.push(elapsed_txs.as_secs_f64() * 1000.0);
            }
            Err(err) => {
                error!("block {} not saved: {}", produced_block.slot, err);
            }
        }
    } // -- END for

    total_save_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    block_save_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    transactions_save_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    info!("total save times histogram(ms): {}", solana_lite_rpc_util::histogram_percentiles::calculate_percentiles(&total_save_times));
    info!("block save times histogram(ms): {}", solana_lite_rpc_util::histogram_percentiles::calculate_percentiles(&block_save_times));
    info!("txs save times histogram(ms): {}", solana_lite_rpc_util::histogram_percentiles::calculate_percentiles(&transactions_save_times));

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

