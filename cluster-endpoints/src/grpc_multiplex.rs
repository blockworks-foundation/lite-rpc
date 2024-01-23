use crate::grpc_subscription::{
    map_block_update,
};
use anyhow::{bail, Context};
use futures::{Stream, StreamExt};
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task;
use geyser_grpc_connector::grpcmultiplex_fastestwins::{
    FromYellowstoneExtractor,
};
use log::{debug, info, trace, warn};
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::structures::slot_notification::SlotNotification;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;
use geyser_grpc_connector::{Message, GeyserFilter, GrpcSourceConfig};
use geyser_grpc_connector::grpc_subscription_autoreconnect_streams::create_geyser_reconnecting_stream;
use itertools::Itertools;
use merge_streams::MergeStreams;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::AbortHandle;
use tokio_stream::wrappers::ReceiverStream;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterSlots, SubscribeUpdate};

struct BlockExtractor(CommitmentConfig);

impl FromYellowstoneExtractor for BlockExtractor {
    type Target = ProducedBlock;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(Slot, Self::Target)> {
        match update.update_oneof {
            Some(UpdateOneof::Block(update_block_message)) => {
                let block = map_block_update(update_block_message, self.0);
                Some((block.slot, block))
            }
            _ => None,
        }
    }
}

struct BlockMetaHashExtractor(CommitmentConfig);

impl FromYellowstoneExtractor for BlockMetaHashExtractor {
    type Target = String;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(u64, String)> {
        match update.update_oneof {
            Some(UpdateOneof::BlockMeta(block_meta)) => {
                Some((block_meta.slot, block_meta.blockhash))
            }
            _ => None,
        }
    }
}

fn map_slot_from_yellowstone_update(update: SubscribeUpdate) -> Option<Slot> {
    match update.update_oneof {
        Some(UpdateOneof::Slot(update_slot_message)) => {
            Some(update_slot_message.slot)
        }
        _ => None,
    }
}

fn map_block_from_yellowstone_update(update: SubscribeUpdate, commitment_config: CommitmentConfig) -> Option<(Slot, ProducedBlock)> {
    match update.update_oneof {
        Some(UpdateOneof::Block(update_block_message)) => {
            let block = map_block_update(update_block_message, commitment_config);
            Some((block.slot, block))
        }
        _ => None,
    }
}

/// connect to all sources provided using transparent autoconnection task
/// shutdown handling:
/// - task will shutdown of the receiver side of block_sender gets closed
/// - will also shutdown the grpc autoconnection task(s)
fn create_grpc_multiplex_processed_block_stream(
    grpc_sources: &Vec<GrpcSourceConfig>,
    block_sender: UnboundedSender<ProducedBlock>,
) -> Vec<AbortHandle> {
    let commitment_config = CommitmentConfig::processed();

    let mut channels = vec![];
    for grpc_source in grpc_sources {
        // tasks will be shutdown automatically if the channel gets closed
        let (_jh_geyser_task, message_channel) =
            create_geyser_autoconnection_task(grpc_source.clone(),
              GeyserFilter(commitment_config).blocks_and_txs());
        channels.push(message_channel)
    }

    let source_channels = channels.into_iter().map(ReceiverStream::new).collect_vec();
    let mut fused_streams = source_channels.merge();

    let jh_merging_streams =
        tokio::task::spawn(async move {
        let mut slots_processed = BTreeSet::<u64>::new();
        loop {
            const MAX_SIZE: usize = 1024;
            match fused_streams.next().await {
                Some(Message::GeyserSubscribeUpdate(subscribe_update)) => {
                    let mapfilter = map_block_from_yellowstone_update(*subscribe_update, commitment_config);
                    if let Some((slot, produced_block)) = mapfilter {
                        let commitment_level_block = produced_block.commitment_config.commitment;
                        // check if the slot is in the map, if not check if the container is half full and the slot in question is older than the lowest value
                        // it means that the slot is too old to process
                        if !slots_processed.contains(&slot)
                            && (slots_processed.len() < MAX_SIZE / 2
                            || slot > slots_processed.first().cloned().unwrap_or_default())
                        {
                            let send_result = block_sender
                                .send(produced_block)
                                .context("Send block to channel");
                            if send_result.is_err() {
                                warn!("Block channel receiver is closed - aborting");
                                return;
                            }

                            trace!("emitted block #{}@{} from multiplexer", slot, commitment_level_block);

                            slots_processed.insert(slot);
                            if slots_processed.len() > MAX_SIZE {
                                slots_processed.pop_first();
                            }
                        }
                    }
                }
                Some(Message::Connecting(attempt)) => {
                    if attempt > 1 {
                        warn!("Multiplexed geyser stream performs reconnect attempt {}", attempt);
                    }
                }
                None => {
                    warn!("Multiplexed geyser source stream terminated - aborting task");
                    return;
                }
            }
        } // -- END receiver loop
    });
    vec![jh_merging_streams.abort_handle()]
}

fn create_grpc_multiplex_block_meta_stream(
    grpc_sources: &Vec<GrpcSourceConfig>,
    commitment_config: CommitmentConfig,
) -> impl Stream<Item = String> {

    let mut channels = vec![];
    for grpc_source in grpc_sources {
        let (_jh_geyser_task, message_channel) =
            create_geyser_autoconnection_task(grpc_source.clone(),
                      GeyserFilter(commitment_config).blocks_meta());
        channels.push(message_channel)
    }

    let source_channels = channels.into_iter().map(ReceiverStream::new).collect_vec();

    assert!(commitment_config != CommitmentConfig::processed(), "fastestwins strategy must not be used for processed level");
    geyser_grpc_connector::grpcmultiplex_fastestwins::create_multiplexed_stream(source_channels, BlockMetaHashExtractor(commitment_config))
}

/// connect to multiple grpc sources to consume processed blocks and block status update
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

                let processed_blocks_tasks =
                    create_grpc_multiplex_processed_block_stream(&grpc_sources, processed_block_sender);

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

                let mut cleanup_tick = tokio::time::interval(Duration::from_secs(5));
                let mut last_finalized_slot: Slot = 0;
                let mut cleanup_without_recv_blocks: u8 = 0;
                let mut cleanup_without_confirmed_recv_blocks_meta: u8 = 0;
                let mut cleanup_without_finalized_recv_blocks_meta: u8 = 0;
                let mut confirmed_block_not_yet_processed = HashSet::<String>::new();

                //  start logging errors when we recieve first finalized block
                let mut finalized_block_recieved = false;
                const MAX_ALLOWED_CLEANUP_WITHOUT_RECV: u8 = 12; // 12*5 = 60s without recving data
                loop {
                    tokio::select! {
                        processed_block = processed_block_reciever.recv() => {
                            cleanup_without_recv_blocks = 0;

                            let processed_block = processed_block.expect("processed block from stream");
                            trace!("got processed block {} with blockhash {}",
                                processed_block.slot, processed_block.blockhash.clone());
                            if let Err(e) = producedblock_sender.send(processed_block.clone()) {
                                warn!("produced block channel has no receivers {e:?}");
                                continue
                            }
                            if confirmed_block_not_yet_processed.remove(&processed_block.blockhash) {
                                if let Err(e) = producedblock_sender.send(processed_block.to_confirmed_block()) {
                                    warn!("produced block channel has no receivers {e:?}");
                                    continue
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
                                    warn!("Finalized block channel has no receivers {e:?}");
                                    continue;
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
                                finalized_block_recieved = true;
                                debug!("got finalized blockmeta {} with blockhash {}",
                                    finalized_block.slot, finalized_block.blockhash.clone());
                                if let Err(e) = producedblock_sender.send(finalized_block) {
                                    warn!("Finalized block channel has no receivers {e:?}");
                                    continue;
                                }
                            } else if finalized_block_recieved {
                                // this warning is ok for first few blocks when we start lrpc
                                log::error!("finalized block meta received for blockhash {} which was never seen or already emitted", blockhash);
                            }
                        },
                        _ = cleanup_tick.tick() => {
                            if cleanup_without_finalized_recv_blocks_meta > MAX_ALLOWED_CLEANUP_WITHOUT_RECV ||
                                cleanup_without_recv_blocks > MAX_ALLOWED_CLEANUP_WITHOUT_RECV ||
                                cleanup_without_confirmed_recv_blocks_meta > MAX_ALLOWED_CLEANUP_WITHOUT_RECV {
                                log::error!("block or block meta stream stopped restaring blocks");
                                break;
                            }
                            cleanup_without_recv_blocks += 1;
                            cleanup_without_finalized_recv_blocks_meta += 1;
                            cleanup_without_confirmed_recv_blocks_meta += 1;
                            let size_before = recent_processed_blocks.len();
                            recent_processed_blocks.retain(|_blockhash, block| {
                                last_finalized_slot == 0 || block.slot > last_finalized_slot - 100
                            });
                            let cnt_cleaned = size_before - recent_processed_blocks.len();
                            if cnt_cleaned > 0 {
                                debug!("cleaned {} confirmed blocks from cache", cnt_cleaned);
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

pub fn create_grpc_multiplex_processed_slots_subscription(
    grpc_sources: Vec<GrpcSourceConfig>,
) -> (Receiver<SlotNotification>, AnyhowJoinHandle) {
    const COMMITMENT_CONFIG: CommitmentConfig = CommitmentConfig::processed();
    info!("Setup grpc multiplexed slots connection...");
    if grpc_sources.is_empty() {
        info!("- no grpc connection configured");
    }
    for grpc_source in &grpc_sources {
        info!("- connection to {}", grpc_source);
    }

    let (multiplexed_messages_sender, multiplexed_messages_rx) =
        tokio::sync::broadcast::channel(1000);

    let jh_multiplex_task = tokio::spawn(async move {
        'reconnect_loop: loop {

            let mut tasks: Vec<AbortHandle> = Vec::new();
            let mut channels = vec![];
            for grpc_source in &grpc_sources {
                // tasks will be shutdown automatically if the channel gets closed
                let (_jh_geyser_task, message_channel) =
                    create_geyser_autoconnection_task(grpc_source.clone(),
                          GeyserFilter(COMMITMENT_CONFIG).slots());
                channels.push(message_channel)
            }

            let source_channels = channels.into_iter().map(ReceiverStream::new).collect_vec();
            let mut fused_streams = source_channels.merge();

            // TODO handle cases
            'recv_loop: while let Ok(slot_update) = tokio::time::timeout(
                Duration::from_secs(30),
                fused_streams.next()).await {
                if let Some(Message::GeyserSubscribeUpdate(slot_update)) = slot_update {
                    let mapfilter = map_slot_from_yellowstone_update(*slot_update);
                    if let Some(slot) = mapfilter {
                        let send_result = multiplexed_messages_sender.send(SlotNotification {
                            processed_slot: slot,
                            estimated_processed_slot: slot,
                        }).context("Send slot to channel");
                        if send_result.is_err() {
                            warn!("Slot channel receiver is closed - aborting");
                            bail!("Slot channel receiver is closed - aborting");
                        }

                        trace!("emitted slot #{}@{} from multiplexer",
                                slot, COMMITMENT_CONFIG.commitment);
                    }
                }
            }

        }
    });

    (multiplexed_messages_rx, jh_multiplex_task)
}
