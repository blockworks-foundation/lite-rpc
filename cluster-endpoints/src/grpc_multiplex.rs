use crate::grpc_subscription::from_grpc_block_update;
use anyhow::{bail, Context};
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::grpcmultiplex_fastestwins::FromYellowstoneExtractor;
use geyser_grpc_connector::{GeyserFilter, GrpcSourceConfig, Message};
use log::{debug, info, trace, warn};
use solana_lite_rpc_core::structures::produced_block::{ProducedBlock, ProducedBlockShared};
use solana_lite_rpc_core::structures::slot_notification::SlotNotification;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use debug_collections::hashmap_wrapped::HashMap;
use tokio::sync::broadcast::Receiver;
use tokio::task::AbortHandle;
use tokio::time::sleep;
use tracing::debug_span;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;
use debug_collections::tokio_wrapped::mpsc::channels_wrapped::send_timed;

/// connect to all sources provided using transparent autoconnection task
/// shutdown handling:
/// - task will shutdown of the receiver side of block_sender gets closed
/// - will also shutdown the grpc autoconnection task(s)
fn create_grpc_multiplex_processed_block_task(
    grpc_sources: &Vec<GrpcSourceConfig>,
    block_sender: tokio::sync::mpsc::Sender<ProducedBlockShared>,
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
                    // TIME SPENT IN CHANNEL: 18.450375ms
                    // TIME SPENT IN CHANNEL: 25.791µs
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
                            let send_result =
                                send_timed(produced_block, &block_sender)
                                .await
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

// backpressure: the mpsc sender will block grpc stream until capacity is available
fn create_grpc_multiplex_block_meta_task(
    grpc_sources: &Vec<GrpcSourceConfig>,
    block_meta_sender: tokio::sync::mpsc::Sender<BlockMeta>,
    commitment_config: CommitmentConfig,
) -> Vec<AbortHandle> {
    let (autoconnect_tx, mut blocks_rx) = tokio::sync::mpsc::channel(10);
    for grpc_source in grpc_sources {
        create_geyser_autoconnection_task_with_mpsc(
            grpc_source.clone(),
            GeyserFilter(commitment_config).blocks_meta(),
            autoconnect_tx.clone(),
        );
    }

    let jh_merging_streams = tokio::task::spawn(async move {
        let mut tip: Slot = 0;
        loop {
            match blocks_rx.recv().await {
                Some(Message::GeyserSubscribeUpdate(subscribe_update)) => {
                    if let Some(update) = subscribe_update.update_oneof {
                        match update {
                            UpdateOneof::BlockMeta(block_meta) => {
                                let proposed_slot = block_meta.slot;
                                if proposed_slot > tip {
                                    tip = proposed_slot;
                                    let block_meta = BlockMeta {
                                        slot: proposed_slot,
                                        blockhash: block_meta.blockhash,
                                    };

                                    let send_started_at = Instant::now();
                                    let send_result =
                                        send_timed(block_meta, &block_meta_sender)
                                        .await
                                        .context("Send block to channel");
                                    if send_result.is_err() {
                                        warn!("Block channel receiver is closed - aborting");
                                        return;
                                    }

                                    trace!(
                                        "emitted block meta #{}@{} from multiplexer took {:?}",
                                        proposed_slot,
                                        commitment_config.commitment,
                                        send_started_at.elapsed()
                                    );
                                }
                            }
                            _other_message => {
                                trace!("ignoring message")
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
                    warn!("Multiplexed geyser source stream block meta terminated - aborting task");
                    return;
                }
            }
        } // -- END receiver loop
    });

    vec![jh_merging_streams.abort_handle()]
}

/// connect to multiple grpc sources to consume processed blocks and block status update
/// emits full blocks for commitment levels processed, confirmed, finalized in that order
/// the channel must never be closed
pub fn create_grpc_multiplex_blocks_subscription(
    grpc_sources: Vec<GrpcSourceConfig>,
) -> (Receiver<ProducedBlockShared>, AnyhowJoinHandle) {
    info!("Setup grpc multiplexed blocks connection...");
    if grpc_sources.is_empty() {
        info!("- no grpc connection configured");
    }
    for grpc_source in &grpc_sources {
        info!("- connection to {}", grpc_source);
    }

    // return value is the broadcast receiver
    // must NEVER be closed form inside this method
    let (producedblock_sender, blocks_output_stream) =
        tokio::sync::broadcast::channel::<ProducedBlockShared>(32);

    let mut reconnect_attempts = 0;

    // task MUST not terminate but might be aborted from outside
    let jh_block_emitter_task = tokio::task::spawn(async move {
        loop {
            // channels must NEVER GET CLOSED (unless full restart of multiplexer)
            let (processed_block_sender, mut processed_block_reciever) =
                tokio::sync::mpsc::channel::<ProducedBlockShared>(10); // experiemental
            let (block_meta_sender_confirmed, mut block_meta_reciever_confirmed) =
                tokio::sync::mpsc::channel::<BlockMeta>(500);
            let (block_meta_sender_finalized, mut block_meta_reciever_finalized) =
                tokio::sync::mpsc::channel::<BlockMeta>(500);

            let processed_block_sender = processed_block_sender.clone();
            reconnect_attempts += 1;
            if reconnect_attempts > 1 {
                warn!(
                    "Multiplexed geyser stream performs reconnect attempt {}",
                    reconnect_attempts
                );
            }

            // tasks which should be cleaned up uppon reconnect
            let mut task_list: Vec<AbortHandle> = vec![];

            let processed_blocks_tasks = create_grpc_multiplex_processed_block_task(
                &grpc_sources,
                processed_block_sender.clone(),
            );
            task_list.extend(processed_blocks_tasks);

            // TODO apply same pattern as in create_grpc_multiplex_processed_block_task
            let jh_meta_task_confirmed = create_grpc_multiplex_block_meta_task(
                &grpc_sources,
                block_meta_sender_confirmed.clone(),
                CommitmentConfig::confirmed(),
            );
            task_list.extend(jh_meta_task_confirmed);
            let jh_meta_task_finalized = create_grpc_multiplex_block_meta_task(
                &grpc_sources,
                block_meta_sender_finalized.clone(),
                CommitmentConfig::finalized(),
            );
            task_list.extend(jh_meta_task_finalized);

            // by blockhash
            // this map consumes sigificant amount of memory constrainted by CLEANUP_SLOTS_BEHIND_FINALIZED
            let mut recent_processed_blocks = HashMap::<String, ProducedBlockShared>::new_with_warn_threshold(3);

            let mut cleanup_tick = tokio::time::interval(Duration::from_secs(5));
            let mut last_finalized_slot: Slot = 0;
            const CLEANUP_SLOTS_BEHIND_FINALIZED: u64 = 100;
            let mut cleanup_without_recv_full_blocks: u8 = 0;
            let mut cleanup_without_confirmed_recv_blocks_meta: u8 = 0;
            let mut cleanup_without_finalized_recv_blocks_meta: u8 = 0;
            let mut confirmed_block_not_yet_processed = HashSet::<String>::new();

            //  start logging errors when we recieve first finalized block
            let mut startup_completed = false;
            const MAX_ALLOWED_CLEANUP_WITHOUT_RECV: u8 = 12; // 12*5 = 60s without recving data
            'recv_loop: loop {
                debug!("processed_block_sender: {}, block_meta_sender_confirmed: {}, block_meta_sender_finalized: {}",
                    processed_block_sender.capacity(),
                    block_meta_sender_confirmed.capacity(),
                    block_meta_sender_finalized.capacity()
                );
                tokio::select! {
                    processed_block = processed_block_reciever.recv() => {
                            cleanup_without_recv_full_blocks = 0;

                            let processed_block = processed_block.expect("processed block from stream");
                            trace!("got processed block {} with blockhash {}",
                                processed_block.slot, processed_block.blockhash.clone());
                            if let Err(e) = producedblock_sender.send(processed_block.clone()) {
                                warn!("produced block channel has no receivers {e:?}");
                            }
                            if confirmed_block_not_yet_processed.remove(&processed_block.blockhash) {
                                if let Err(e) = producedblock_sender.send(Arc::new(processed_block.to_confirmed_block())) {
                                    warn!("produced block channel has no receivers while trying to send confirmed block {e:?}");
                                }
                            }
                            recent_processed_blocks.insert(processed_block.blockhash.clone(), processed_block);
                        },
                        meta_confirmed = block_meta_reciever_confirmed.recv() => {
                            cleanup_without_confirmed_recv_blocks_meta = 0;
                            let meta_confirmed = meta_confirmed.expect("confirmed block meta from stream");
                            let blockhash = meta_confirmed.blockhash;
                            if let Some(cached_processed_block) = recent_processed_blocks.get(&blockhash) {
                                let confirmed_block = Arc::new(cached_processed_block.to_confirmed_block());
                                debug!("got confirmed blockmeta {} with blockhash {}",
                                    confirmed_block.slot, confirmed_block.blockhash.clone());
                                if let Err(e) = producedblock_sender.send(confirmed_block) {
                                    warn!("confirmed block channel has no receivers {e:?}");
                                }
                            } else {
                                confirmed_block_not_yet_processed.insert(blockhash.clone());
                                log::debug!("backlog of not yet confirmed blocks: {}; recent blocks map size: {}",
                                confirmed_block_not_yet_processed.len(), recent_processed_blocks.len());
                            }
                        },
                        meta_finalized = block_meta_reciever_finalized.recv() => {
                            cleanup_without_finalized_recv_blocks_meta = 0;
                            let meta_finalized = meta_finalized.expect("finalized block meta from stream");
                            // let _span = debug_span!("sequence_block_meta_finalized", ?meta_finalized.slot).entered();
                            let blockhash = meta_finalized.blockhash;
                            if let Some(cached_processed_block) = recent_processed_blocks.remove(&blockhash) {
                                let finalized_block = Arc::new(cached_processed_block.to_finalized_block());
                                last_finalized_slot = finalized_block.slot;
                                startup_completed = true;
                                debug!("got finalized blockmeta {} with blockhash {}",
                                    finalized_block.slot, finalized_block.blockhash.clone());
                                if let Err(e) = producedblock_sender.send(finalized_block) {
                                    warn!("Finalized block channel has no receivers {e:?}");
                                }
                            } else if startup_completed {
                                // this warning is ok for first few blocks when we start lrpc
                                log::warn!("finalized block meta received for blockhash {} which was never seen or already emitted", blockhash);
                            }
                        },
                    _ = cleanup_tick.tick() => {
                         // timebased restart
                        if cleanup_without_recv_full_blocks > MAX_ALLOWED_CLEANUP_WITHOUT_RECV ||
                            cleanup_without_confirmed_recv_blocks_meta > MAX_ALLOWED_CLEANUP_WITHOUT_RECV ||
                            cleanup_without_finalized_recv_blocks_meta > MAX_ALLOWED_CLEANUP_WITHOUT_RECV {
                            log::error!("block or block meta geyser stream stopped - restarting multiplexer ({}-{}-{})",
                            cleanup_without_recv_full_blocks, cleanup_without_confirmed_recv_blocks_meta, cleanup_without_finalized_recv_blocks_meta,);
                            // throttle a bit
                            sleep(Duration::from_millis(1500)).await;
                            break 'recv_loop;
                        }
                        cleanup_without_recv_full_blocks += 1;
                        cleanup_without_confirmed_recv_blocks_meta += 1;
                        cleanup_without_finalized_recv_blocks_meta += 1;
                        let size_before = recent_processed_blocks.len();
                        recent_processed_blocks.retain(|_blockhash, block| {
                            last_finalized_slot == 0 || block.slot > last_finalized_slot.saturating_sub(CLEANUP_SLOTS_BEHIND_FINALIZED)
                        });
                        let cnt_cleaned = size_before.saturating_sub(recent_processed_blocks.len());
                        if cnt_cleaned > 0 {
                            debug!("cleaned {} processed blocks from cache", cnt_cleaned);
                        }
                    }
                }
            } // -- END receiver loop
            task_list.iter().for_each(|task| task.abort());
        } // -- END reconnect loop
    });

    (blocks_output_stream, jh_block_emitter_task)
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

    // multiplexed_messages_sender must not be closed from inside this method
    let (multiplexed_messages_sender, multiplexed_messages_rx) =
        tokio::sync::broadcast::channel(32);

    // task MUST not terminate but might be aborted from outside
    let jh_multiplex_task = tokio::spawn(async move {
        loop {
            let (autoconnect_tx, mut slots_rx) = tokio::sync::mpsc::channel(10);
            for grpc_source in &grpc_sources {
                create_geyser_autoconnection_task_with_mpsc(
                    grpc_source.clone(),
                    GeyserFilter(COMMITMENT_CONFIG).slots(),
                    autoconnect_tx.clone(),
                );
            }

            'recv_loop: loop {
                let next = tokio::time::timeout(Duration::from_secs(30), slots_rx.recv()).await;
                match next {
                    Ok(Some(Message::GeyserSubscribeUpdate(slot_update))) => {
                        let mapfilter = map_slot_from_yellowstone_update(*slot_update);
                        if let Some(slot) = mapfilter {
                            let _span = debug_span!("grpc_multiplex_processed_slots_stream", ?slot)
                                .entered();
                            let send_started_at = Instant::now();
                            let send_result = multiplexed_messages_sender
                                .send(SlotNotification {
                                    processed_slot: slot,
                                    estimated_processed_slot: slot,
                                })
                                .context("Send slot to channel");
                            if send_result.is_err() {
                                warn!("Slot channel receiver is closed - aborting");
                                bail!("Slot channel receiver is closed - aborting");
                            }

                            trace!(
                                "emitted slot #{}@{} from multiplexer took {:?}",
                                slot,
                                COMMITMENT_CONFIG.commitment,
                                send_started_at.elapsed()
                            );
                        }
                    }
                    Ok(Some(Message::Connecting(attempt))) => {
                        if attempt > 1 {
                            warn!(
                                "Multiplexed geyser slot stream performs reconnect attempt {}",
                                attempt
                            );
                        }
                    }
                    Ok(None) => {
                        warn!("Multiplexed geyser source stream slot terminated - reconnect");
                        break 'recv_loop;
                    }
                    Err(_elapsed) => {
                        warn!("Multiplexed geyser slot stream timeout - reconnect");
                        // throttle
                        sleep(Duration::from_millis(1500)).await;
                        break 'recv_loop;
                    }
                }
            } // -- END receiver loop
        } // -- END reconnect loop
    });

    (multiplexed_messages_rx, jh_multiplex_task)
}

#[allow(dead_code)]
struct BlockMeta {
    pub slot: Slot,
    pub blockhash: String,
}

struct BlockMetaExtractor(CommitmentConfig);

impl FromYellowstoneExtractor for BlockMetaExtractor {
    type Target = BlockMeta;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(u64, BlockMeta)> {
        match update.update_oneof {
            Some(UpdateOneof::BlockMeta(block_meta)) => Some((
                block_meta.slot,
                BlockMeta {
                    slot: block_meta.slot,
                    blockhash: block_meta.blockhash,
                },
            )),
            _ => None,
        }
    }
}

fn map_slot_from_yellowstone_update(update: SubscribeUpdate) -> Option<Slot> {
    match update.update_oneof {
        Some(UpdateOneof::Slot(update_slot_message)) => Some(update_slot_message.slot),
        _ => None,
    }
}

fn map_block_from_yellowstone_update(
    update: SubscribeUpdate,
    commitment_config: CommitmentConfig,
) -> Option<(Slot, ProducedBlockShared)> {
    let _span = debug_span!("map_block_from_yellowstone_update").entered();
    match update.update_oneof {
        Some(UpdateOneof::Block(update_block_message)) => {
            let started_at = std::time::Instant::now();
            let block = Arc::new(from_grpc_block_update(update_block_message, commitment_config));
            debug!(
                "MAPPING block from yellowstone with {} txs update took {:?}",
                block.transactions.len(),
                started_at.elapsed()
            );
            debug!("MAPPING block from yellowstone with {} txs update took {:?}",
                block.transactions.len(),
                started_at.elapsed());
            // sleep_350(started_at);
            // debug!("SLEEP+MAPPING block from yellowstone with {} txs took {:?}",
            //     block.transactions.len(),
            //     started_at.elapsed());
            Some((block.slot, block))
        }
        _ => None,
    }
}

fn sleep_350(started_at: std::time::Instant) {
    while std::time::Instant::now().duration_since(started_at) < Duration::from_millis(350)  {
        std::thread::sleep(Duration::from_millis(10));
    }
}
