use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use solana_lite_rpc_core::types::BlockStream;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::{env, io, process, thread};
use std::fs::{create_dir, DirBuilder, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use bytes::Bytes;
use geyser_grpc_connector::{GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig};
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task;
use tokio::io::BufWriter;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::{sleep, Instant};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;
use yellowstone_grpc_proto::prost::Message;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;




#[test]
pub fn read_block_from_disk() {
    tracing_subscriber::fmt::init();

    let blockstream_dumpdir = BlockStreamDumpOnDisk::new_with_existing_directory(Path::new("/Users/stefan/mango/code/lite-rpc/localdev-groovie-testing/blocks_on_disk"));

    let block_file = blockstream_dumpdir.build_path(&BlockStorageMeta {
        slot: 251395041,
        epoch_ms: 1707312285514,
    });

    let (slot_from_file, epoch_ms) = parse_slot_and_timestamp_from_file(block_file.file_name().unwrap().to_str().unwrap());
    info!("slot from file: {}", slot_from_file);
    info!("epochms from file: {}", epoch_ms);

    let block_bytes = std::fs::read(block_file).unwrap();
    info!("read {} bytes from block file", block_bytes.len());
    let decoded = SubscribeUpdateBlock::decode(block_bytes.as_slice()).expect("Block file must be protobuf");


    info!("decoded block: {}", decoded.slot);
}

// e.g. block-000251395041-confirmed-1707312285514.dat
fn parse_slot_and_timestamp_from_file(file_name: &str) -> (Slot, u64) {
    let slot_str = file_name.split("-").nth(1).unwrap();
    let slot = slot_str.parse::<Slot>().unwrap();
    let timestamp_str = file_name.split("-").nth(3).unwrap().split(".").next().unwrap();
    info!("parsed slot {} from file name", slot);
    let epoch_ms = timestamp_str.parse::<u64>().unwrap();
    (slot, epoch_ms)
}




// note: we assume that the invariants hold even right after startup
pub async fn spawn_block_todisk_writer(
    mut block_notifier: tokio::sync::mpsc::Receiver<geyser_grpc_connector::Message>,
    block_dump_base_directory: PathBuf,
) -> AbortHandle {
    let block_dump_base_directory = block_dump_base_directory.to_path_buf();
    let join_handle = tokio::spawn(async move {
        let blockstream_dumpdir = BlockStreamDumpOnDisk::new_with_existing_directory(block_dump_base_directory.as_path());
        loop {
            match block_notifier.recv().await {
                Some(message) => {
                    match message {
                        geyser_grpc_connector::Message::GeyserSubscribeUpdate(subscriber_update) => {
                            if let Some(UpdateOneof::Block(block_update)) = subscriber_update.update_oneof {
                                info!("got block update -> slot: {}", block_update.slot);
                                blockstream_dumpdir.write_geyser_update(&block_update);
                            }
                        }
                        geyser_grpc_connector::Message::Connecting(attempt) => {
                            warn!("Connection attempt: {}", attempt);
                        }
                    }
                }
                None => {
                    error!("Stream closed - shutting down task");
                    return;
                }
            }
        }
    });
    join_handle.abort_handle()
}


pub struct BlockStreamDumpOnDisk {
    root_path: PathBuf,
}

// 1707307827380
pub fn now_epoch_ms() -> u64{
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64
}


struct BlockStorageMeta {
    slot: Slot,
    epoch_ms: u64,
}

impl BlockStorageMeta {
    pub fn new_with_now(slot: Slot) -> Self {
        let epoch_ms = now_epoch_ms();
        Self { slot, epoch_ms }
    }
}

impl BlockStreamDumpOnDisk {
    // note: the directory must exist and marker file must be present!
    pub fn new_with_existing_directory(root_path: &Path) -> Self {
        let marker = Path::new(".solana-blocks-dump");
        assert!(
            root_path.join(marker).is_file(),
            "Expecting directory {} with marker file ({})",
            root_path.display(),
            marker.display()
        );
        Self {
            root_path: root_path.to_path_buf(),
        }
    }

    // /blocks-000246300xxx/block-000246300234-confirmed-1707308047927.dat
    fn build_path(&self, block_storage_meta: &BlockStorageMeta) -> PathBuf {
        let block_path = self.root_path
            // pad with "0" to allow sorting
            .join(format!("blocks-{slot_xxx:0>12}", slot_xxx=Self::format_slot_1k(block_storage_meta.slot)))
            .join(format!("block-{slot:0>12}-confirmed-{epoch_ms}.dat", slot=block_storage_meta.slot, epoch_ms=block_storage_meta.epoch_ms));
        block_path
    }

    fn write_block(&self, meta: &BlockStorageMeta, block_protobuf: &SubscribeUpdateBlock) {
        let block_file = self.build_path(meta);
        let block_dir = block_file.parent();

        let create_result = create_dir(block_dir.unwrap());
        if create_result.is_err() {
            if create_result.err().unwrap().kind() != io::ErrorKind::AlreadyExists {
                panic!("Must be able to create directory: {:?}", block_dir.unwrap());
            }
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
        info!("Wrote block {} with {} bytes to disk: {}", meta.slot, block_bytes.len(), block_file.display());
    }

    pub fn write_geyser_update(&self, block_update: &SubscribeUpdateBlock) {
        let mut buf = vec![];
        block_update.encode(&mut buf).expect("must be able to serialize to buffer");
        let meta = BlockStorageMeta {
            slot: block_update.slot,
            epoch_ms: now_epoch_ms(),
        };
        self.write_block(&meta, &block_update);
    }

    // 246300234 -> 246300xxx
    pub fn format_slot_1k(slot: Slot) -> String {
        format!("{}xxx", slot / 1000)
    }

}




#[test]
fn format_slot_xxx() {
    assert_eq!("246300xxx", BlockStreamDumpOnDisk::format_slot_1k(246300234));
}

#[test]
fn parse_slot_and_epochms() {
    let file_name = "block-000251395041-confirmed-1707312285514.dat";
    let (slot_from_file, epoch_ms) = parse_slot_and_timestamp_from_file(file_name);
    info!("slot from file: {}", slot_from_file);
    info!("epochms from file: {}", epoch_ms);

}


fn configure_panic_hook() {
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        default_panic(panic_info);
        // e.g. panicked at 'BANG', lite-rpc/tests/blockstore_integration_tests:260:25
        error!("{}", panic_info);
        eprintln!("{}", panic_info);
        process::exit(12);
    }));
}
