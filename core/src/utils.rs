use std::time::Duration;

use log::debug;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::time::{timeout, Instant};

use crate::{structures::block_info::BlockInfo, types::BlockInfoStream};

pub async fn get_latest_block_info(
    mut blockinfo_stream: BlockInfoStream,
    commitment_config: CommitmentConfig,
) -> BlockInfo {
    let started = Instant::now();
    loop {
        match timeout(Duration::from_millis(500), blockinfo_stream.recv()).await {
            Ok(Ok(block_info)) => {
                if block_info.commitment_config == commitment_config {
                    return block_info;
                }
            }
            Err(_elapsed) => {
                debug!(
                    "waiting for latest block info ({}) ... {:.02}ms",
                    commitment_config.commitment,
                    started.elapsed().as_secs_f32() * 1000.0
                );
            }
            Ok(Err(_error)) => {
                panic!("Did not recv block info");
            }
        }
    }
}
