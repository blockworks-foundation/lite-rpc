use anyhow::{bail, Context};
use geyser_grpc_connector::grpc_subscription_autoreconnect_tasks::create_geyser_autoconnection_task_with_mpsc;
use geyser_grpc_connector::{GeyserFilter, GrpcSourceConfig, Message};
use itertools::Itertools;
use log::{debug, info, trace, warn};
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::structures::slot_notification::SlotNotification;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;

use solana_lite_rpc_core::solana_utils::hash_from_str;
use solana_lite_rpc_core::structures::block_info::BlockInfo;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;
use tokio::sync::broadcast::{self, Receiver};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Instant};
use tracing::debug_span;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::SubscribeUpdate;

use crate::grpc::grpc_subscription::from_grpc_block_update;

/// connect to all sources provided using transparent autoconnection task
/// shutdown handling:
/// - task will shutdown of the receiver side of block_sender gets closed
/// - will also shutdown the grpc autoconnection task(s)
fn create_grpc_multiplex_processed_block_task(
    grpc_sources: &Vec<GrpcSourceConfig>,
    block_sender: tokio::sync::mpsc::Sender<ProducedBlock>,
    mut exit_notify: broadcast::Receiver<()>,
) -> Vec<JoinHandle<()>> {
    const COMMITMENT_CONFIG: CommitmentConfig = CommitmentConfig::processed();

    let (autoconnect_tx, mut blocks_rx) = tokio::sync::mpsc::channel(10);
    let mut tasks = vec![];

    for grpc_source in grpc_sources {
        let task = create_geyser_autoconnection_task_with_mpsc(
            grpc_source.clone(),
            GeyserFilter(COMMITMENT_CONFIG).blocks_and_txs(),
            autoconnect_tx.clone(),
            exit_notify.resubscribe(),
        );
        tasks.push(task);
    }

    let jh_merging_streams = tokio::task::spawn(async move {
        let mut slots_processed = BTreeSet::<u64>::new();
        let mut last_tick = Instant::now();
        'recv_loop: loop {
            // recv loop
            if last_tick.elapsed() > Duration::from_millis(800) {
                trace!(
                    "(soft_realtime) slow multiplex loop interation: {:?}",
                    last_tick.elapsed()
                );
            }
            last_tick = Instant::now();

            const MAX_SIZE: usize = 1024;
            let blocks_rx_result = tokio::select! {
                res = blocks_rx.recv() => {
                    res
                },
                _ = exit_notify.recv() => {
                    break;
                }
            };
            match blocks_rx_result {
                Some(Message::GeyserSubscribeUpdate(subscribe_update)) => {
                    // note: avoid mapping of full block as long as possible
                    let extracted_slot = extract_slot_from_yellowstone_update(&subscribe_update);
                    if let Some(slot) = extracted_slot {
                        // check if the slot is in the map, if not check if the container is half full and the slot in question is older than the lowest value
                        // it means that the slot is too old to process
                        if slots_processed.contains(&slot) {
                            continue 'recv_loop;
                        }
                        if slots_processed.len() >= MAX_SIZE / 2
                            && slot <= slots_processed.first().cloned().unwrap_or_default()
                        {
                            continue 'recv_loop;
                        }

                        let mapfilter =
                            map_block_from_yellowstone_update(*subscribe_update, COMMITMENT_CONFIG);
                        if let Some((_slot, produced_block)) = mapfilter {
                            let send_started_at = Instant::now();
                            let send_result = block_sender
                                .send(produced_block)
                                .await
                                .context("Send block to channel");
                            if send_result.is_err() {
                                warn!("Block channel receiver is closed - aborting");
                                break;
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
                    break;
                }
            }
        } // -- END receiver loop
    });
    tasks.push(jh_merging_streams);
    tasks
}

// backpressure: the mpsc sender will block grpc stream until capacity is available
fn create_grpc_multiplex_block_info_task(
    grpc_sources: &Vec<GrpcSourceConfig>,
    block_info_sender: tokio::sync::mpsc::Sender<BlockInfo>,
    commitment_config: CommitmentConfig,
    mut exit_notify: broadcast::Receiver<()>,
) -> Vec<JoinHandle<()>> {
    let (autoconnect_tx, mut blocks_rx) = tokio::sync::mpsc::channel(10);
    let mut tasks = vec![];
    for grpc_source in grpc_sources {
        let task = create_geyser_autoconnection_task_with_mpsc(
            grpc_source.clone(),
            GeyserFilter(commitment_config).blocks_meta(),
            autoconnect_tx.clone(),
            exit_notify.resubscribe(),
        );
        tasks.push(task);
    }

    let jh_merging_streams = tokio::task::spawn(async move {
        let mut tip: Slot = 0;
        loop {
            let blocks_rx_result = tokio::select! {
                res = blocks_rx.recv() => {
                    res
                },
                _ = exit_notify.recv() => {
                    break;
                }
            };
            match blocks_rx_result {
                Some(Message::GeyserSubscribeUpdate(subscribe_update)) => {
                    if let Some(update) = subscribe_update.update_oneof {
                        match update {
                            UpdateOneof::BlockMeta(block_meta) => {
                                let proposed_slot = block_meta.slot;
                                if proposed_slot > tip {
                                    tip = proposed_slot;
                                    let block_meta = BlockInfo {
                                        slot: proposed_slot,
                                        block_height: block_meta
                                            .block_height
                                            .expect("block_height from geyser block meta")
                                            .block_height,
                                        blockhash: hash_from_str(&block_meta.blockhash)
                                            .expect("valid blockhash"),
                                        commitment_config,
                                        block_time: block_meta
                                            .block_time
                                            .expect("block_time from geyser block meta")
                                            .timestamp
                                            as u64,
                                    };

                                    let send_started_at = Instant::now();
                                    let send_result = block_info_sender
                                        .send(block_meta)
                                        .await
                                        .context("Send block to channel");
                                    if send_result.is_err() {
                                        warn!("Block channel receiver is closed - aborting");
                                        break;
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
                    break;
                }
            }
        } // -- END receiver loop
    });
    tasks.push(jh_merging_streams);
    tasks
}

/// connect to multiple grpc sources to consume processed blocks and block status update
/// emits full blocks for commitment levels processed, confirmed, finalized in that order
/// the channel must never be closed
pub fn create_grpc_multiplex_blocks_subscription(
    grpc_sources: Vec<GrpcSourceConfig>,
) -> (
    Receiver<ProducedBlock>,
    Receiver<BlockInfo>,
    AnyhowJoinHandle,
) {
    info!("Setup grpc multiplexed blocks connection...");
    if grpc_sources.is_empty() {
        info!("- no grpc connection configured");
    }
    for grpc_source in &grpc_sources {
        info!("- connection to {}", grpc_source);
    }

    // return value is the broadcast receiver
    // must NEVER be closed from inside this method
    let (producedblock_sender, blocks_output_stream) =
        tokio::sync::broadcast::channel::<ProducedBlock>(32);
    // provide information about finalized blocks as quickly as possible
    // note that produced block stream might most probably lag behind
    let (blockinfo_sender, blockinfo_output_stream) =
        tokio::sync::broadcast::channel::<BlockInfo>(32);

    let mut reconnect_attempts = 0;

    // task MUST not terminate but might be aborted from outside
    let jh_block_emitter_task = tokio::task::spawn(async move {
        loop {
            // channels must NEVER GET CLOSED (unless full restart of multiplexer)
            let (processed_block_sender, mut processed_block_reciever) =
                tokio::sync::mpsc::channel::<ProducedBlock>(10); // experiemental
            let (block_info_sender_processed, mut block_info_reciever_processed) =
                tokio::sync::mpsc::channel::<BlockInfo>(500);
            let (block_info_sender_confirmed, mut block_info_reciever_confirmed) =
                tokio::sync::mpsc::channel::<BlockInfo>(500);
            let (block_info_sender_finalized, mut block_info_reciever_finalized) =
                tokio::sync::mpsc::channel::<BlockInfo>(500);
            let (exit_sender, exit_notify) = broadcast::channel(1);

            let processed_block_sender = processed_block_sender.clone();
            reconnect_attempts += 1;
            if reconnect_attempts > 1 {
                warn!(
                    "Multiplexed geyser stream performs reconnect attempt {}",
                    reconnect_attempts
                );
            }

            // tasks which should be cleaned up uppon reconnect
            let mut task_list: Vec<JoinHandle<()>> = vec![];

            let processed_blocks_tasks = create_grpc_multiplex_processed_block_task(
                &grpc_sources,
                processed_block_sender.clone(),
                exit_notify.resubscribe(),
            );
            task_list.extend(processed_blocks_tasks);

            // TODO apply same pattern as in create_grpc_multiplex_processed_block_task

            let jh_meta_task_processed = create_grpc_multiplex_block_info_task(
                &grpc_sources,
                block_info_sender_processed.clone(),
                CommitmentConfig::processed(),
                exit_notify.resubscribe(),
            );
            task_list.extend(jh_meta_task_processed);
            let jh_meta_task_confirmed = create_grpc_multiplex_block_info_task(
                &grpc_sources,
                block_info_sender_confirmed.clone(),
                CommitmentConfig::confirmed(),
                exit_notify.resubscribe(),
            );
            task_list.extend(jh_meta_task_confirmed);
            let jh_meta_task_finalized = create_grpc_multiplex_block_info_task(
                &grpc_sources,
                block_info_sender_finalized.clone(),
                CommitmentConfig::finalized(),
                exit_notify,
            );
            task_list.extend(jh_meta_task_finalized);

            // by blockhash
            // this map consumes sigificant amount of memory constrainted by CLEANUP_SLOTS_BEHIND_FINALIZED
            let mut recent_processed_blocks =
                HashMap::<solana_sdk::hash::Hash, ProducedBlock>::new();

            let mut cleanup_tick = tokio::time::interval(Duration::from_secs(5));
            let mut last_finalized_slot: Slot = 0;
            const CLEANUP_SLOTS_BEHIND_FINALIZED: u64 = 100;
            let mut cleanup_without_recv_full_blocks: u8 = 0;
            let mut cleanup_without_confirmed_recv_blocks_meta: u8 = 0;
            let mut cleanup_without_finalized_recv_blocks_meta: u8 = 0;
            let mut confirmed_block_not_yet_processed = HashSet::<solana_sdk::hash::Hash>::new();
            let mut finalized_block_not_yet_processed = HashSet::<solana_sdk::hash::Hash>::new();

            //  start logging errors when we recieve first finalized block
            let mut startup_completed = false;
            const MAX_ALLOWED_CLEANUP_WITHOUT_RECV: u8 = 12; // 12*5 = 60s without recving data
            'recv_loop: loop {
                debug!("channel capacities: processed_block_sender={}, block_info_sender_confirmed={}, block_info_sender_finalized={}",
                    processed_block_sender.capacity(),
                    block_info_sender_confirmed.capacity(),
                    block_info_sender_finalized.capacity()
                );
                tokio::select! {
                    processed_block = processed_block_reciever.recv() => {
                            cleanup_without_recv_full_blocks = 0;

                            let processed_block = processed_block.expect("processed block from stream");
                            trace!("got processed block {} with blockhash {}",
                                processed_block.slot, processed_block.blockhash.clone());

                            if processed_block.commitment_config.is_finalized() {
                                 last_finalized_slot = last_finalized_slot.max(processed_block.slot);
                            }

                            if let Err(e) = producedblock_sender.send(processed_block.clone()) {
                                warn!("produced block channel has no receivers {e:?}");
                            }
                            if confirmed_block_not_yet_processed.remove(&processed_block.blockhash) {
                                if let Err(e) = producedblock_sender.send(processed_block.to_confirmed_block()) {
                                    warn!("produced block channel has no receivers while trying to send confirmed block {e:?}");
                                }
                            }
                            if finalized_block_not_yet_processed.remove(&processed_block.blockhash) {
                                if let Err(e) = producedblock_sender.send(processed_block.to_finalized_block()) {
                                    warn!("produced block channel has no receivers while trying to send confirmed block {e:?}");
                                }
                            }
                            recent_processed_blocks.insert(processed_block.blockhash, processed_block);

                        },
                        blockinfo_processed = block_info_reciever_processed.recv() => {
                            let blockinfo_processed = blockinfo_processed.expect("processed block info from stream");
                            let blockhash = blockinfo_processed.blockhash;
                             trace!("got processed blockinfo {} with blockhash {}",
                                blockinfo_processed.slot, blockhash);
                            if let Err(e) = blockinfo_sender.send(blockinfo_processed) {
                                warn!("Processed blockinfo channel has no receivers {e:?}");
                            }
                        },
                        blockinfo_confirmed = block_info_reciever_confirmed.recv() => {
                            cleanup_without_confirmed_recv_blocks_meta = 0;
                            let blockinfo_confirmed = blockinfo_confirmed.expect("confirmed block info from stream");
                            let blockhash = blockinfo_confirmed.blockhash;
                             trace!("got confirmed blockinfo {} with blockhash {}",
                                blockinfo_confirmed.slot, blockhash);
                            if let Err(e) = blockinfo_sender.send(blockinfo_confirmed) {
                                warn!("Confirmed blockinfo channel has no receivers {e:?}");
                            }

                            if let Some(cached_processed_block) = recent_processed_blocks.get(&blockhash) {
                                let confirmed_block = cached_processed_block.to_confirmed_block();
                                debug!("got confirmed blockinfo {} with blockhash {}",
                                    confirmed_block.slot, confirmed_block.blockhash.clone());
                                if let Err(e) = producedblock_sender.send(confirmed_block) {
                                    warn!("confirmed block channel has no receivers {e:?}");
                                }
                            } else {
                                confirmed_block_not_yet_processed.insert(blockhash);
                                log::debug!("backlog of not yet confirmed blocks: {}; recent blocks map size: {}",
                                confirmed_block_not_yet_processed.len(), recent_processed_blocks.len());
                            }
                        },
                        blockinfo_finalized = block_info_reciever_finalized.recv() => {
                            cleanup_without_finalized_recv_blocks_meta = 0;
                            let blockinfo_finalized = blockinfo_finalized.expect("finalized block info from stream");
                            last_finalized_slot = last_finalized_slot.max(blockinfo_finalized.slot);

                            let blockhash = blockinfo_finalized.blockhash;
                             trace!("got finalized blockinfo {} with blockhash {}",
                                blockinfo_finalized.slot, blockhash);
                            if let Err(e) = blockinfo_sender.send(blockinfo_finalized) {
                                warn!("Finalized blockinfo channel has no receivers {e:?}");
                            }

                            if let Some(cached_processed_block) = recent_processed_blocks.remove(&blockhash) {
                                let finalized_block = cached_processed_block.to_finalized_block();
                                startup_completed = true;
                                debug!("got finalized blockinfo {} with blockhash {}",
                                    finalized_block.slot, finalized_block.blockhash.clone());
                                if let Err(e) = producedblock_sender.send(finalized_block) {
                                    warn!("Finalized block channel has no receivers {e:?}");
                                }
                            } else if startup_completed {
                                // this warning is ok for first few blocks when we start lrpc
                                log::warn!("finalized blockinfo received for blockhash {} which was never seen or already emitted", blockhash);
                                finalized_block_not_yet_processed.insert(blockhash);
                            }
                        },
                    _ = cleanup_tick.tick() => {
                         // timebased restart
                        if cleanup_without_recv_full_blocks > MAX_ALLOWED_CLEANUP_WITHOUT_RECV ||
                            cleanup_without_confirmed_recv_blocks_meta > MAX_ALLOWED_CLEANUP_WITHOUT_RECV ||
                            cleanup_without_finalized_recv_blocks_meta > MAX_ALLOWED_CLEANUP_WITHOUT_RECV {
                            log::error!("block or block info geyser stream stopped - restarting multiplexer ({}-{}-{})",
                            cleanup_without_recv_full_blocks, cleanup_without_confirmed_recv_blocks_meta, cleanup_without_finalized_recv_blocks_meta,);
                            // throttle a bit
                            sleep(Duration::from_millis(200)).await;
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
            if exit_sender.send(()).is_ok() {
                futures::future::join_all(task_list).await;
            } else {
                log::error!("Problem sending exit signal");
                task_list.iter().for_each(|x| x.abort());
            }
        } // -- END reconnect loop
    });

    (
        blocks_output_stream,
        blockinfo_output_stream,
        jh_block_emitter_task,
    )
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
            let (exit_sender, exit_notify) = broadcast::channel(1);

            let task_list = grpc_sources
                .clone()
                .iter()
                .map(|grpc_source| {
                    create_geyser_autoconnection_task_with_mpsc(
                        grpc_source.clone(),
                        GeyserFilter(COMMITMENT_CONFIG).slots(),
                        autoconnect_tx.clone(),
                        exit_notify.resubscribe(),
                    )
                })
                .collect_vec();

            let mut last_slot = 0;
            'recv_loop: loop {
                let next = tokio::time::timeout(Duration::from_secs(30), slots_rx.recv()).await;
                match next {
                    Ok(Some(Message::GeyserSubscribeUpdate(slot_update))) => {
                        let mapfilter = map_slot_from_yellowstone_update(*slot_update);
                        if let Some(slot) = mapfilter {
                            if last_slot > slot {
                                continue;
                            }
                            last_slot = slot;

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

            if exit_sender.send(()).is_ok() {
                futures::future::join_all(task_list).await;
            } else {
                log::error!("Problem sending exit signal");
                task_list.iter().for_each(|x| x.abort());
            }
        } // -- END reconnect loop
    });

    (multiplexed_messages_rx, jh_multiplex_task)
}

fn extract_slot_from_yellowstone_update(update: &SubscribeUpdate) -> Option<Slot> {
    match &update.update_oneof {
        // list is not exhaustive
        Some(UpdateOneof::Slot(update_message)) => Some(update_message.slot),
        Some(UpdateOneof::BlockMeta(update_message)) => Some(update_message.slot),
        Some(UpdateOneof::Block(update_message)) => Some(update_message.slot),
        _ => None,
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
) -> Option<(Slot, ProducedBlock)> {
    let _span = debug_span!("map_block_from_yellowstone_update").entered();
    match update.update_oneof {
        Some(UpdateOneof::Block(update_block_message)) => {
            let block = from_grpc_block_update(update_block_message, commitment_config);
            Some((block.slot, block))
        }
        _ => None,
    }
}
