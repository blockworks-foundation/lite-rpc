use crate::grpc_subscription::{create_block_processing_task, map_block_update};
use anyhow::{bail, Context};
use futures::StreamExt;
use geyser_grpc_connector::grpc_subscription_autoreconnect::{
    create_geyser_reconnecting_stream, GeyserFilter, GrpcSourceConfig,
};
use geyser_grpc_connector::grpcmultiplex_fastestwins::{
    create_multiplexed_stream, FromYellowstoneExtractor,
};
use log::{debug, info, trace, warn};
use merge_streams::MergeStreams;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::structures::slot_notification::SlotNotification;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::{BTreeSet, HashMap};
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterSlots, SubscribeUpdate,
};

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

/// connect to multiple grpc sources to consume confirmed blocks and block status update
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
                let (confirmed_block_sender, mut confirmed_block_reciever) =
                    tokio::sync::mpsc::unbounded_channel::<ProducedBlock>();
                let _confirmed_blocks_tasks = {
                    let commitment_config = CommitmentConfig::confirmed();

                    let mut tasks = Vec::new();
                    let mut streams = vec![];
                    for grpc_source in &grpc_sources {
                        let (block_sender, block_reciever) = async_channel::unbounded();
                        tasks.push(create_block_processing_task(
                            grpc_source.grpc_addr.clone(),
                            grpc_source.grpc_x_token.clone(),
                            block_sender,
                            yellowstone_grpc_proto::geyser::CommitmentLevel::Confirmed,
                        ));
                        streams.push(block_reciever)
                    }
                    let merging_streams: AnyhowJoinHandle = tokio::task::spawn(async move {
                        let mut slots_processed = BTreeSet::<u64>::new();
                        loop {
                            let block_message =
                                futures::stream::select_all(streams.clone()).next().await;
                            const MAX_SIZE: usize = 1024;
                            if let Some(block) = block_message {
                                let slot = block.slot;
                                // check if the slot is in the map, if not check if the container is half full and the slot in question is older than the lowest value
                                // it means that the slot is too old to process
                                if !slots_processed.contains(&slot)
                                    && (slots_processed.len() < MAX_SIZE / 2
                                        || slot
                                            > slots_processed.first().cloned().unwrap_or_default())
                                {
                                    confirmed_block_sender
                                        .send(map_block_update(block, commitment_config))
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
                };

                let finalized_blockmeta_stream = {
                    let commitment_config = CommitmentConfig::finalized();

                    let mut streams = Vec::new();
                    for grpc_source in &grpc_sources {
                        let stream = create_geyser_reconnecting_stream(
                            grpc_source.clone(),
                            GeyserFilter(commitment_config).blocks_meta(),
                        );
                        streams.push(stream);
                    }
                    create_multiplexed_stream(streams, BlockMetaHashExtractor(commitment_config))
                };

                // by blockhash
                let mut recent_confirmed_blocks = HashMap::<String, ProducedBlock>::new();
                let mut finalized_blockmeta_stream = std::pin::pin!(finalized_blockmeta_stream);

                let mut cleanup_tick = tokio::time::interval(Duration::from_secs(5));
                let mut last_finalized_slot: Slot = 0;
                let mut cleanup_without_recv_blocks: u8 = 0;
                let mut cleanup_without_recv_blocks_meta: u8 = 0;
                const MAX_ALLOWED_CLEANUP_WITHOUT_RECV: u8 = 12; // 12*5 = 60s without recving data
                loop {
                    tokio::select! {
                        confirmed_block = confirmed_block_reciever.recv() => {
                            cleanup_without_recv_blocks = 0;

                            let confirmed_block = confirmed_block.expect("confirmed block from stream");
                            trace!("got confirmed block {} with blockhash {}",
                                confirmed_block.slot, confirmed_block.blockhash.clone());
                            if let Err(e) = producedblock_sender.send(confirmed_block.clone()) {
                                warn!("Confirmed block channel has no receivers {e:?}");
                                continue
                            }
                            recent_confirmed_blocks.insert(confirmed_block.blockhash.clone(), confirmed_block);
                        },
                        meta_finalized = finalized_blockmeta_stream.next() => {
                            cleanup_without_recv_blocks_meta = 0;
                            let blockhash = meta_finalized.expect("finalized block meta from stream");
                            if let Some(cached_confirmed_block) = recent_confirmed_blocks.remove(&blockhash) {
                                let finalized_block = cached_confirmed_block.to_finalized_block();
                                last_finalized_slot = finalized_block.slot;
                                debug!("got finalized blockmeta {} with blockhash {}",
                                    finalized_block.slot, finalized_block.blockhash.clone());
                                if let Err(e) = producedblock_sender.send(finalized_block) {
                                    warn!("Finalized block channel has no receivers {e:?}");
                                    continue;
                                }
                            } else {
                                // this warning is ok for first few blocks when we start lrpc
                                warn!("finalized block meta received for blockhash {} which was never seen or already emitted", blockhash);
                            }
                        },
                        _ = cleanup_tick.tick() => {
                            if cleanup_without_recv_blocks_meta > MAX_ALLOWED_CLEANUP_WITHOUT_RECV ||
                                cleanup_without_recv_blocks > MAX_ALLOWED_CLEANUP_WITHOUT_RECV {
                                log::error!("block or block meta stream stopped restaring blocks");
                                break;
                            }
                            cleanup_without_recv_blocks += 1;
                            cleanup_without_recv_blocks_meta += 1;
                            let size_before = recent_confirmed_blocks.len();
                            recent_confirmed_blocks.retain(|_blockhash, block| {
                                last_finalized_slot == 0 || block.slot > last_finalized_slot - 100
                            });
                            let cnt_cleaned = size_before - recent_confirmed_blocks.len();
                            if cnt_cleaned > 0 {
                                debug!("cleaned {} confirmed blocks from cache", cnt_cleaned);
                            }
                        }
                    }
                }
            }
        })
    };

    (blocks_output_stream, jh_block_emitter_task)
}

pub fn create_grpc_multiplex_processed_blocks_subscription(
    grpc_sources: Vec<GrpcSourceConfig>,
) -> (Receiver<ProducedBlock>, AnyhowJoinHandle) {
    info!("Setup grpc multiplexed processed blocks connection...");
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
            // 'reconnect_loop: loop {
            {
                let commitment_config = CommitmentConfig::processed();

                let mut tasks = Vec::new();
                let mut streams = vec![];
                for grpc_source in &grpc_sources {
                    let (block_sender, block_reciever) = async_channel::unbounded();
                    tasks.push(create_block_processing_task(
                        grpc_source.grpc_addr.clone(),
                        grpc_source.grpc_x_token.clone(),
                        block_sender,
                        yellowstone_grpc_proto::geyser::CommitmentLevel::Processed,
                    ));
                    streams.push(Box::pin(block_reciever));
                }

                let mut merged = streams.merge();

                while let Some(update_block) = merged.next().await {
                    let _todo = producedblock_sender
                        .send(map_block_update(update_block, commitment_config));
                    // TODO handle error
                    // .expect();
                }
            };

            // } -- reconnect loop
            bail!("reconnect loop exited");
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

    let (multiplexed_messages_sender, multiplexed_messages) = tokio::sync::broadcast::channel(1000);

    let jh = tokio::spawn(async move {
        loop {
            let multiplex_stream = {
                let mut streams = Vec::new();
                for grpc_source in &grpc_sources {
                    let mut slots = HashMap::new();
                    slots.insert(
                        "client".to_string(),
                        SubscribeRequestFilterSlots {
                            filter_by_commitment: Some(true),
                        },
                    );

                    let filter = SubscribeRequest {
                        slots,
                        accounts: Default::default(),
                        transactions: HashMap::new(),
                        entry: Default::default(),
                        blocks: HashMap::new(),
                        blocks_meta: HashMap::new(),
                        commitment: Some(
                            yellowstone_grpc_proto::geyser::CommitmentLevel::Processed as i32,
                        ),
                        accounts_data_slice: Default::default(),
                        ping: None,
                    };

                    let stream = create_geyser_reconnecting_stream(grpc_source.clone(), filter);
                    streams.push(stream);
                }

                create_multiplexed_stream(streams, SlotExtractor {})
            };

            let mut multiplex_stream = std::pin::pin!(multiplex_stream);
            loop {
                tokio::select! {
                    slot_data = multiplex_stream.next() => {
                        if let Some(slot_data) = slot_data {
                            match multiplexed_messages_sender.send(slot_data) {
                                Ok(receivers) => {
                                    trace!("sent data to {} receivers", receivers);
                                }
                                Err(send_error) => log::error!("Get error while sending on slot channel {}", send_error),
                            };
                        } else {
                            debug!("Slot stream send None type");
                        }
                    },
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {
                        log::error!("Slots timedout restarting subscription");
                        break;
                    }
                }
            }
        }
    });

    (multiplexed_messages, jh)
}
