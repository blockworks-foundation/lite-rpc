use crate::grpc_subscription::from_grpc_block_update;
use anyhow::{bail, Context};
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::grpcmultiplex_fastestwins::FromYellowstoneExtractor;
use geyser_grpc_connector::{GeyserFilter, GrpcSourceConfig, Message};
use log::{debug, info, trace, warn};
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::structures::slot_notification::SlotNotification;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio::sync::broadcast::Receiver;
use tokio::task::AbortHandle;
use tokio::time::{sleep};
use tracing::debug_span;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;

/// connect to all sources provided using transparent autoconnection task
/// shutdown handling:
/// - task will shutdown of the receiver side of block_sender gets closed
/// - will also shutdown the grpc autoconnection task(s)
pub fn create_grpc_multiplex_processed_block_task(
    grpc_sources: &Vec<GrpcSourceConfig>,
    // changed to unbound
    block_sender: tokio::sync::mpsc::UnboundedSender<ProducedBlock>,
) -> Vec<AbortHandle> {
    const COMMITMENT_CONFIG: CommitmentConfig = CommitmentConfig::processed();

    let (autoconnect_tx, mut blocks_rx) = tokio::sync::mpsc::channel(10);
    for grpc_source in grpc_sources {
        create_geyser_autoconnection_task_with_mpsc(
            grpc_source.clone(),
            GeyserFilter(COMMITMENT_CONFIG).blocks_and_txs(),
            autoconnect_tx.clone(),
        );
    }

    let jh_merging_streams = tokio::task::spawn(async move {
        let mut slots_processed = BTreeSet::<u64>::new();
        let mut last_tick = Instant::now();
        loop {
            // recv loop
            if last_tick.elapsed() > Duration::from_millis(200) {
                warn!(
                    "(soft_realtime) slow multiplex loop interation: {:?}",
                    last_tick.elapsed()
                );
            }
            last_tick = Instant::now();

            const MAX_SIZE: usize = 1024;
            match blocks_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(subscribe_update)) => {
                    let mapfilter =
                        map_block_from_yellowstone_update(*subscribe_update, COMMITMENT_CONFIG);
                    if let Some((slot, produced_block)) = mapfilter {
                        assert_eq!(COMMITMENT_CONFIG, produced_block.commitment_config);
                        // check if the slot is in the map, if not check if the container is half full and the slot in question is older than the lowest value
                        // it means that the slot is too old to process
                        if !slots_processed.contains(&slot)
                            && (slots_processed.len() < MAX_SIZE / 2
                            || slot > slots_processed.first().cloned().unwrap_or_default())
                        {
                            let send_started_at = Instant::now();
                            let send_result = block_sender
                                .send(produced_block)
                                // .await
                                .context("Send block to channel");
                            if send_result.is_err() {
                                warn!("Block channel receiver is closed - aborting");
                                return;
                            }

                            trace!(
                                "emitted block #{}@{} from multiplexer took {:?}",
                                slot,
                                COMMITMENT_CONFIG.commitment,
                                send_started_at.elapsed()
                            );

                            slots_processed.insert(slot);
                            if slots_processed.len() > MAX_SIZE {
                                slots_processed.pop_first();
                            }
                        }
                    }
                }
                Some(Message::Connecting(attempt)) => {
                    if attempt > 1 {
                        warn!(
                            "Multiplexed geyser stream performs reconnect attempt {}",
                            attempt
                        );
                    }
                }
                None => {
                    warn!("Multiplexed geyser source stream block terminated - aborting task");
                    return;
                }
            }
        } // -- END receiver loop
    });
    vec![jh_merging_streams.abort_handle()]
}



fn map_block_from_yellowstone_update(
    update: SubscribeUpdate,
    commitment_config: CommitmentConfig,
) -> Option<(Slot, ProducedBlock)> {
    let _span = debug_span!("map_block_from_yellowstone_update").entered();
    match update.update_oneof {
        Some(UpdateOneof::Block(update_block_message)) => {
            let started_at = std::time::Instant::now();
            let block = from_grpc_block_update(update_block_message, commitment_config);
            debug!("MAPPING block from yellowstone with {} txs update took {:?}",
                block.transactions.len(),
                started_at.elapsed());
            Some((block.slot, block))
        }
        _ => None,
    }
}

