use crate::grpc_subscription::{
    create_block_processing_task, create_slot_stream_task, from_grpc_block_update,
};
use anyhow::Context;
use futures::{Stream, StreamExt};
use geyser_grpc_connector::grpc_subscription_autoreconnect::{
    create_geyser_reconnecting_stream, GeyserFilter, GrpcSourceConfig,
};
use geyser_grpc_connector::grpcmultiplex_fastestwins::{
    create_multiplexed_stream, FromYellowstoneExtractor,
};
use log::{debug, info, trace, warn};
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::structures::slot_notification::SlotNotification;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::UnboundedSender;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use tracing::{debug_span, instrument, trace_span, warn_span};

struct BlockExtractor(CommitmentConfig);

impl FromYellowstoneExtractor for BlockExtractor {
    type Target = ProducedBlock;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)> {
        match update.update_oneof {
            Some(UpdateOneof::Block(update_block_message)) => {
                let block = from_grpc_block_update(update_block_message, self.0);
                Some((block.slot, block))
            }
            _ => None,
        }
    }
}

struct BlockMetaHashExtractor(CommitmentConfig);

impl FromYellowstoneExtractor for BlockMetaHashExtractor {
    type Target = String;
    #[tracing::instrument(skip_all)]
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(u64, String)> {
        match update.update_oneof {
            Some(UpdateOneof::BlockMeta(block_meta)) => {
                Some((block_meta.slot, block_meta.blockhash))
            }
            _ => None,
        }
    }
}

fn create_grpc_multiplex_processed_block_stream(
    grpc_sources: &Vec<GrpcSourceConfig>,
    processed_block_sender: UnboundedSender<ProducedBlock>,
) -> Vec<AnyhowJoinHandle> {
    let commitment_config = CommitmentConfig::processed();

    let mut tasks = Vec::new();
    let mut streams = vec![];
    for grpc_source in grpc_sources {
        let (block_sender, block_reciever) = async_channel::unbounded();
        tasks.push(create_block_processing_task(
            grpc_source.grpc_addr.clone(),
            grpc_source.grpc_x_token.clone(),
            block_sender,
            yellowstone_grpc_proto::geyser::CommitmentLevel::Processed,
        ));
        streams.push(block_reciever)
    }
    let merging_streams: AnyhowJoinHandle = tokio::task::spawn(async move {
        let mut slots_processed = BTreeSet::<u64>::new();
        loop {
            let block_message = futures::stream::select_all(streams.clone()).next().await;
            const MAX_SIZE: usize = 1024;
            if let Some(block) = block_message {
                debug_span!("grpc_multiplex_processed_block_stream", ?block.slot);
                let slot = block.slot;
                // check if the slot is in the map, if not check if the container is half full and the slot in question is older than the lowest value
                // it means that the slot is too old to process
                if !slots_processed.contains(&slot)
                    && (slots_processed.len() < MAX_SIZE / 2
                        || slot > slots_processed.first().cloned().unwrap_or_default())
                {
                    processed_block_sender
                        .send(from_grpc_block_update(block, commitment_config))
                        .context("Issue to send confirmed block")?;
                    slots_processed.insert(slot);
                    if slots_processed.len() > MAX_SIZE {
                        slots_processed.pop_first();
                    }
                }
            }
        }
    });
    tasks.push(merging_streams);
    tasks
}

fn create_grpc_multiplex_block_meta_stream(
    grpc_sources: &Vec<GrpcSourceConfig>,
    commitment_config: CommitmentConfig,
) -> impl Stream<Item = String> {
    let mut streams = Vec::new();
    for grpc_source in grpc_sources {
        let stream = create_geyser_reconnecting_stream(
            grpc_source.clone(),
            GeyserFilter(commitment_config).blocks_meta(),
        );
        streams.push(stream);
    }
    create_multiplexed_stream(streams, BlockMetaHashExtractor(commitment_config))
}

/// connect to multiple grpc sources to consume processed (full) blocks and block meta for commitment level confirmed and finalized
/// will emit blocks for commitment level processed, confirmed and finalized OR only processed block never gets confirmed
pub fn create_grpc_multiplex_blocks_subscription(
    grpc_sources: Vec<GrpcSourceConfig>,
) -> (Receiver<ProducedBlock>, AnyhowJoinHandle) {
    info!("Setup grpc multiplexed blocks connection...");
    if grpc_sources.is_empty() {
        info!("- no grpc connection configured");
    }
    for grpc_source in &grpc_sources {
        info!("- connection to {}", grpc_source);
    }

    // return value is the broadcast receiver
    let (producedblock_sender, blocks_output_stream) =
        tokio::sync::broadcast::channel::<ProducedBlock>(1000);

    let jh_block_emitter_task = {
        tokio::task::spawn(async move {
            loop {
                let (processed_block_sender, mut processed_block_reciever) =
                    tokio::sync::mpsc::unbounded_channel::<ProducedBlock>();

                let processed_blocks_tasks = create_grpc_multiplex_processed_block_stream(
                    &grpc_sources,
                    processed_block_sender,
                );

                let confirmed_blockmeta_stream = create_grpc_multiplex_block_meta_stream(
                    &grpc_sources,
                    CommitmentConfig::confirmed(),
                );
                let finalized_blockmeta_stream = create_grpc_multiplex_block_meta_stream(
                    &grpc_sources,
                    CommitmentConfig::finalized(),
                );

                // by blockhash
                let mut recent_processed_blocks = HashMap::<String, ProducedBlock>::new();
                let mut confirmed_blockmeta_stream = std::pin::pin!(confirmed_blockmeta_stream);
                let mut finalized_blockmeta_stream = std::pin::pin!(finalized_blockmeta_stream);

                let mut last_finalized_slot: Slot = 0;
                let mut cleanup_tick = tokio::time::interval(Duration::from_secs(5));
                const MAX_ALLOWED_CLEANUP_WITHOUT_RECV: u32 = 12; // 12*5 = 60s without recving data
                const CLEANUP_SLOTS_BEHIND_FINALIZED: u64 = 100;
                let mut cleanup_without_recv_blocks: u32 = 0;
                let mut cleanup_without_confirmed_recv_blocks_meta: u32 = 0;
                let mut cleanup_without_finalized_recv_blocks_meta: u32 = 0;
                let mut confirmed_block_not_yet_processed = HashSet::<String>::new();

                //  start logging errors when we recieve first finalized block
                let mut startup_completed = false;
                loop {
                    tokio::select! {
                        processed_block = processed_block_reciever.recv() => {
                            cleanup_without_recv_blocks = 0;


                            let processed_block = processed_block.expect("processed block from stream");
                            trace!("got processed block {} with blockhash {}",
                                processed_block.slot, processed_block.blockhash.clone());
                            if let Err(e) = producedblock_sender.send(processed_block.clone()) {
                                warn!("produced block channel has no receivers {e:?}");
                            }
                            if confirmed_block_not_yet_processed.remove(&processed_block.blockhash) {
                                if let Err(e) = producedblock_sender.send(processed_block.to_confirmed_block()) {
                                    warn!("produced block channel has no receivers {e:?}");
                                }
                            }
                            recent_processed_blocks.insert(processed_block.blockhash.clone(), processed_block);
                        },
                        meta_confirmed = confirmed_blockmeta_stream.next() => {
                            cleanup_without_confirmed_recv_blocks_meta = 0;
                            let blockhash = meta_confirmed.expect("confirmed block meta from stream");
                            if let Some(cached_processed_block) = recent_processed_blocks.get(&blockhash) {
                                let confirmed_block = cached_processed_block.to_confirmed_block();
                                debug!("got confirmed blockmeta {} with blockhash {}",
                                    confirmed_block.slot, confirmed_block.blockhash.clone());
                                if let Err(e) = producedblock_sender.send(confirmed_block) {
                                    warn!("Confirmed block channel has no receivers {e:?}");
                                }
                            } else {
                                confirmed_block_not_yet_processed.insert(blockhash.clone());
                                log::debug!("confirmed blocks not found : {}", confirmed_block_not_yet_processed.len());
                            }
                        },
                        meta_finalized = finalized_blockmeta_stream.next() => {
                            cleanup_without_finalized_recv_blocks_meta = 0;
                            let blockhash = meta_finalized.expect("finalized block meta from stream");
                            if let Some(cached_processed_block) = recent_processed_blocks.remove(&blockhash) {
                                let finalized_block = cached_processed_block.to_finalized_block();
                                last_finalized_slot = finalized_block.slot;
                                startup_completed = true;
                                debug!("got finalized blockmeta {} with blockhash {}",
                                    finalized_block.slot, finalized_block.blockhash.clone());
                                if let Err(e) = producedblock_sender.send(finalized_block) {
                                    warn!("Finalized block channel has no receivers {e:?}");
                                }
                            } else if startup_completed {
                                // this warning is ok for first few blocks when we start lrpc
                                log::error!("finalized block meta received for blockhash {} which was never seen or already emitted", blockhash);
                            }
                        },
                        _ = cleanup_tick.tick() => {
                            // timebased restart
                            if
                                cleanup_without_finalized_recv_blocks_meta > MAX_ALLOWED_CLEANUP_WITHOUT_RECV
                                && cleanup_without_recv_blocks > MAX_ALLOWED_CLEANUP_WITHOUT_RECV
                                && cleanup_without_confirmed_recv_blocks_meta > MAX_ALLOWED_CLEANUP_WITHOUT_RECV {
                                log::error!("block or block meta stream stopped restaring blocks");
                                break;
                            }
                            cleanup_without_recv_blocks += 1;
                            cleanup_without_finalized_recv_blocks_meta += 1;
                            cleanup_without_confirmed_recv_blocks_meta += 1;
                            let size_before = recent_processed_blocks.len();
                            recent_processed_blocks.retain(|_blockhash, block| {
                                last_finalized_slot == 0 || block.slot > last_finalized_slot - CLEANUP_SLOTS_BEHIND_FINALIZED
                            });
                            let cnt_cleaned = size_before - recent_processed_blocks.len();
                            if cnt_cleaned > 0 {
                                debug!("cleaned {} processed blocks from cache", cnt_cleaned);
                            }
                        }
                    }
                }
                // abort all the tasks
                processed_blocks_tasks.iter().for_each(|task| task.abort());
            }
        })
    };

    (blocks_output_stream, jh_block_emitter_task)
}

struct SlotExtractor {}

impl FromYellowstoneExtractor for crate::grpc_multiplex::SlotExtractor {
    type Target = SlotNotification;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)> {
        match update.update_oneof {
            Some(UpdateOneof::Slot(update_slot_message)) => {
                let slot = SlotNotification {
                    estimated_processed_slot: update_slot_message.slot,
                    processed_slot: update_slot_message.slot,
                };
                Some((update_slot_message.slot, slot))
            }
            _ => None,
        }
    }
}

pub fn create_grpc_multiplex_slots_subscription(
    grpc_sources: Vec<GrpcSourceConfig>,
) -> (Receiver<SlotNotification>, AnyhowJoinHandle) {
    info!("Setup grpc multiplexed slots connection...");
    if grpc_sources.is_empty() {
        info!("- no grpc connection configured");
    }
    for grpc_source in &grpc_sources {
        info!("- connection to {}", grpc_source);
    }

    let (multiplexed_messages_sender, multiplexed_messages_rx) =
        tokio::sync::broadcast::channel(1000);

    let jh = tokio::spawn(async move {
        loop {
            let mut streams_tasks = Vec::new();
            let mut recievers = Vec::new();
            for grpc_source in &grpc_sources {
                let (sx, rx) = async_channel::unbounded();
                let task = create_slot_stream_task(
                    grpc_source.grpc_addr.clone(),
                    grpc_source.grpc_x_token.clone(),
                    sx,
                    yellowstone_grpc_proto::geyser::CommitmentLevel::Processed,
                );
                streams_tasks.push(task);
                recievers.push(rx);
            }

            while let Ok(slot_update) = tokio::time::timeout(
                Duration::from_secs(30),
                futures::stream::select_all(recievers.clone()).next(),
            )
            .await
            {
                if let Some(slot_update) = slot_update {
                    multiplexed_messages_sender.send(SlotNotification {
                        processed_slot: slot_update.slot,
                        estimated_processed_slot: slot_update.slot,
                    })?;
                }
            }

            streams_tasks.iter().for_each(|task| task.abort());
        }
    });

    (multiplexed_messages_rx, jh)
}
