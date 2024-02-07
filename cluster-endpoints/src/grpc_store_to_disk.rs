use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use solana_lite_rpc_core::types::BlockStream;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
use std::{env, thread};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use bytes::Bytes;
use geyser_grpc_connector::{GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig};
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Instant};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::prost::Message;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;

#[tokio::test]
pub async fn read_stream() {
    tracing_subscriber::fmt::init();

    let grpc_addr = env::var("GRPC_ADDR").unwrap();
    let grpc_x_token = Some(env::var("GRPC_X_TOKEN").unwrap());
    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
        receive_timeout: Duration::from_secs(5),
    };
    let grpc_source_config = GrpcSourceConfig::new(grpc_addr, grpc_x_token, None, timeouts);

    let commitment_config = CommitmentConfig::confirmed();

    let (_jh_geyser_task, mut message_channel) = create_geyser_autoconnection_task(
        grpc_source_config.clone(),
        GeyserFilter(commitment_config).blocks_and_txs(),
    );

    tokio::spawn(async move {
        loop {
            let now = Instant::now();
            match message_channel.recv().await {
                Some(message) => {
                    match message {
                        geyser_grpc_connector::Message::GeyserSubscribeUpdate(subscriber_update) => {
                            if let Some(UpdateOneof::Block(block_update)) = subscriber_update.update_oneof {
                                info!("got block update -> slot: {}", block_update.slot);
                                let mut buf = vec![];
                                block_update.encode(&mut buf).expect("must be able to serialize to buffer");
                                info!("got block update - {} bytes", buf.len());
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

    // wait a bit
    sleep(Duration::from_secs(10)).await;

}



// note: we assume that the invariants hold even right after startup
pub fn spawn_block_todisk_writer(
    mut block_notifier: BlockStream,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        'recv_loop: loop {
            match block_notifier.recv().await {
                Ok(block) => {
                    info!("Received block #{} from notifier", block.slot);




                }

                Err(RecvError::Lagged(missed_blocks)) => {
                    warn!(
                        "Could not keep up with producer - missed {} blocks",
                        missed_blocks
                    );
                }
                Err(RecvError::Closed) => {
                    info!("Channel was closed - aborting");
                    break 'recv_loop;
                }
            }

        } // -- END receiver loop
        info!("Geyser block todisk task for confirmation sequence shutting down.")
    })
}


pub struct ArchiveOnLocalDisk {
    root_path: PathBuf,
}

// 1707307827380
pub fn epoch_ms()  -> u64{
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64
}


struct BlockStorageMeta {
    slot: Slot,
    epoch_ms: u64,
}

impl BlockStorageMeta {
    pub fn new_with_now(slot: Slot) -> Self {
        let epoch_ms = epoch_ms();
        Self { slot, epoch_ms }
    }
}

impl ArchiveOnLocalDisk {
    pub fn create_with_root_path(root_path: &Path) -> Self {
        let marker = Path::new(".solana-blocks-dump");
        assert!(
            root_path.join(marker).exists(),
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

    // 246300234 -> 246300xxx
    pub fn format_slot_1k(slot: Slot) -> String {
        format!("{}xxx", slot / 1000)
    }

}



#[test]
fn runit() {
    tracing_subscriber::fmt::init();

    let root_path = Path::new("/Users/stefan/mango/code/lite-rpc/localdev-groovie-testing/blocks_on_disk");
    let blocks_on_disk =
        ArchiveOnLocalDisk::create_with_root_path(root_path);

    let block_storage_meta = BlockStorageMeta::new_with_now(246300234);

    info!("blocks_on_disk {:?}", blocks_on_disk.build_path(&block_storage_meta));
}

#[test]
fn format_slot_xxx() {
    assert_eq!("246300xxx", ArchiveOnLocalDisk::format_slot_1k(246300234));
}

