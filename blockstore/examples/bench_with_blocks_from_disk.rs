
use log::{error, info, warn};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::fs::{create_dir, OpenOptions};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tokio::task::AbortHandle;
use tracing::debug;

use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;
use yellowstone_grpc_proto::prost::Message;
use solana_lite_rpc_blockstore::block_stores::postgres::BlockstorePostgresSessionConfig;
use solana_lite_rpc_blockstore::block_stores::postgres::postgres_block_store_writer::PostgresBlockStore;
use solana_lite_rpc_cluster_endpoints::grpc_subscription::from_grpc_block_update;
use solana_lite_rpc_core::structures::epoch::EpochCache;

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let block_dumpdir = std::env::var("BLOCK_DUMPDIR").expect("env var BLOCK_DUMPDIR is mandatory");
    let blockstream_dumpdir = BlockStreamDumpOnDisk::new_with_existing_directory(Path::new(&block_dumpdir));

    let pg_config = BlockstorePostgresSessionConfig::new_from_env("BENCH").unwrap();

    let rpc_url = std::env::var("RPC_URL").expect("env var RPC_URL is mandatory");
    let rpc_client = Arc::new(RpcClient::new(rpc_url));

    let (epoch_cache, _) = EpochCache::bootstrap_epoch(&rpc_client).await.unwrap();

    let block_store = PostgresBlockStore::new(epoch_cache, pg_config).await;



    let block_meta = BlockDumpRef {
        slot: 251395041,
        epoch_ms: 1707312285514,
        commitment_config: CommitmentConfig::confirmed(),
    };
    let block_file = blockstream_dumpdir.build_path(&block_meta);

    let (slot_from_file, epoch_ms) =
        parse_slot_and_timestamp_from_file(block_file.file_name().unwrap().to_str().unwrap());
    info!("slot from file: {}", slot_from_file);
    info!("epochms from file: {}", epoch_ms);

    let block_bytes = std::fs::read(block_file).unwrap();
    info!("read {} bytes from block file", block_bytes.len());
    let decoded: SubscribeUpdateBlock = blockstream_dumpdir.decode_from_file(&block_meta);

    info!("decoded block: {}", decoded.slot);

    let produced_block = from_grpc_block_update(decoded, CommitmentConfig::confirmed());

    block_store.prepare_epoch_schema(produced_block.slot).await.expect("prepare epoch schema must succeed");
    let result = block_store.save_confirmed_block(&produced_block).await;

    info!("result: {:?}", result);

}

fn commitment_short(commitment_config: CommitmentConfig) -> &'static str {
    if commitment_config.is_finalized() {
        "finalized"
    } else if commitment_config.is_confirmed() {
        "confirmed"
    } else if commitment_config.is_processed() {
        "processed"
    } else {
        panic!("commitment config not recognized: {:?}", commitment_config);
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


pub struct BlockStreamDumpOnDisk {
    root_path: PathBuf,
}

// 1707307827380
pub fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub struct BlockDumpRef {
    slot: Slot,
    epoch_ms: u64,
    commitment_config: CommitmentConfig,
}

impl BlockStreamDumpOnDisk {
    // note: the directory must exist and marker file must be present!
    pub fn new_with_existing_directory(root_path: &Path) -> Self {
        let marker = Path::new(".solana-blocks-dump");
        assert!(
            root_path.join(marker).is_file(),
            "Dump directory requires marker file '{}' (directory {})",
            marker.display(),
            root_path.display()
        );
        Self {
            root_path: root_path.to_path_buf(),
        }
    }

    // /blocks-000246300xxx/block-000246300234-confirmed-1707308047927.dat
    fn build_path(&self, block_storage_meta: &BlockDumpRef) -> PathBuf {
        self.root_path
            // pad with "0" to allow sorting
            .join(format!(
                "blocks-{slot_xxx:0>12}",
                slot_xxx = Self::format_slot_1k(block_storage_meta.slot)
            ))
            .join(format!(
                "block-{slot:0>12}-{commitment_short}-{epoch_ms}.dat",
                slot = block_storage_meta.slot,
                commitment_short = commitment_short(block_storage_meta.commitment_config),
                epoch_ms = block_storage_meta.epoch_ms
            ))
    }

    fn write_block(&self, meta: &BlockDumpRef, block_protobuf: &SubscribeUpdateBlock) {
        let block_file = self.build_path(meta);
        let block_dir = block_file.parent();

        let create_result = create_dir(block_dir.unwrap());
        if create_result.is_err()
            && create_result.err().unwrap().kind() != io::ErrorKind::AlreadyExists
        {
            panic!("Must be able to create directory: {:?}", block_dir.unwrap());
        }

        let block_bytes = block_protobuf.encode_to_vec();
        let mut new_block_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(block_file.clone())
            .expect("Must be able to create new file");
        new_block_file
            .write_all(block_bytes.as_slice())
            .expect("must be able to write block to disk");
        info!(
            "Wrote block {} with {} bytes to disk: {}",
            meta.slot,
            block_bytes.len(),
            block_file.display()
        );
    }

    pub fn write_geyser_update(
        &self,
        block_update: &SubscribeUpdateBlock,
        commitment_config: CommitmentConfig,
    ) {
        let mut buf = vec![];
        block_update
            .encode(&mut buf)
            .expect("must be able to serialize to buffer");
        let meta = BlockDumpRef {
            slot: block_update.slot,
            epoch_ms: now_epoch_ms(),
            commitment_config,
        };
        self.write_block(&meta, block_update);
    }

    #[allow(dead_code)]
    pub fn decode_from_file(&self, block_meta: &BlockDumpRef) -> SubscribeUpdateBlock {
        let block_file = self.build_path(block_meta);

        let block_bytes = std::fs::read(block_file).unwrap();
        debug!("read {} bytes from block file", block_bytes.len());
        SubscribeUpdateBlock::decode(block_bytes.as_slice()).expect("Block file must be protobuf")
    }

    // 246300234 -> 246300xxx
    pub fn format_slot_1k(slot: Slot) -> String {
        format!("{}xxx", slot / 1000)
    }
}

#[test]
fn format_slot_xxx() {
    assert_eq!(
        "246300xxx",
        BlockStreamDumpOnDisk::format_slot_1k(246300234)
    );
}

#[test]
fn parse_slot_and_epochms() {
    let file_name = "block-000251395041-confirmed-1707312285514.dat";
    let (slot_from_file, epoch_ms) = parse_slot_and_timestamp_from_file(file_name);
    info!("slot from file: {}", slot_from_file);
    info!("epochms from file: {}", epoch_ms);
}