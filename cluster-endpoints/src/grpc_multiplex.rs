use crate::grpc_subscription::map_block_update;
use futures::{stream, StreamExt};
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
use std::collections::HashMap;
use std::time::Duration;
use tokio::pin;
use tokio::sync::broadcast::Receiver;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SlotParallelization, SubscribeRequest, SubscribeRequestFilterBlocks,
    SubscribeRequestFilterSlots, SubscribeUpdate,
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

    const PARALLELIZATION: i32 = 4;
    let jh_block_emitter_task = {
        tokio::task::spawn(async move {
            loop {
                let (cb_s, cb_r) = async_channel::unbounded::<ProducedBlock>();
                {
                    let commitment_config = CommitmentConfig::confirmed();
                    for grpc_source in &grpc_sources {
                        for id in 0..PARALLELIZATION {
                            let grpc_source = grpc_source.clone();
                            let commitment_config = commitment_config.clone();
                            let cb_s = cb_s.clone();
                            tokio::task::spawn(async move {
                                loop {

                                    let mut blocks_subs = HashMap::new();
                                    blocks_subs.insert(
                                        format!("bk_s_{id:?}"),
                                        SubscribeRequestFilterBlocks {
                                            account_include: Default::default(),
                                            include_transactions: Some(true),
                                            include_accounts: Some(false),
                                            include_entries: Some(false),
                                            slot_parallelization: Some(SlotParallelization {
                                                filter_id: id,
                                                filter_size: PARALLELIZATION,
                                            }),
                                        },
                                    );
                                    // connect to grpc
                                    let mut client = GeyserGrpcClient::connect(
                                        grpc_source.grpc_addr.clone(),
                                        grpc_source.grpc_x_token.clone(),
                                        None,
                                    ).unwrap();
                                    let mut stream = client
                                        .subscribe_once(
                                            HashMap::new(),
                                            Default::default(),
                                            HashMap::new(),
                                            Default::default(),
                                            blocks_subs,
                                            Default::default(),
                                            Some(yellowstone_grpc_proto::geyser::CommitmentLevel::Confirmed),
                                            Default::default(),
                                            None,
                                        )
                                        .await.unwrap();

                                    while let Some(message) = stream.next().await {
                                        let message = message.unwrap();

                                        let Some(update) = message.update_oneof else {
                                            continue;
                                        };

                                        match update {
                                            UpdateOneof::Block(block) => {
                                                let block =
                                                    map_block_update(block, commitment_config);
                                                cb_s.send(block).await.unwrap();
                                            }
                                            UpdateOneof::Ping(_) => {
                                                log::trace!("GRPC Ping");
                                            }
                                            _ => {
                                                log::trace!("unknown GRPC notification");
                                            }
                                        };
                                    }
                                    log::error!("Grpc block subscription broken (resubscribing)");
                                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                }
                            });
                        }
                    }
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
                        confirmed_block = cb_r.recv() => {
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
                                log::error!("finalized block meta received for blockhash {} which was never seen or already emitted", blockhash);
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
