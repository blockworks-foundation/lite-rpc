use crate::grpc_stream_utils::channelize_stream;
use crate::grpc_subscription::map_block_update;
use futures::StreamExt;
use geyser_grpc_connector::grpc_subscription_autoreconnect::{
    create_geyser_reconnecting_stream, GeyserFilter, GrpcSourceConfig,
};
use geyser_grpc_connector::grpcmultiplex_fastestwins::{
    create_multiplexed_stream, FromYellowstoneExtractor,
};
use log::{debug, info, trace};
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::structures::slot_notification::SlotNotification;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::HashMap;
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

    let confirmed_blocks_stream = {
        let commitment_config = CommitmentConfig::confirmed();

        let mut streams = Vec::new();
        for grpc_source in &grpc_sources {
            let stream = create_geyser_reconnecting_stream(
                grpc_source.clone(),
                GeyserFilter(commitment_config).blocks_and_txs(),
            );
            streams.push(stream);
        }

        create_multiplexed_stream(streams, BlockExtractor(commitment_config))
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

    // return value is the broadcast receiver
    let (producedblock_sender, blocks_output_stream) =
        tokio::sync::broadcast::channel::<ProducedBlock>(1000);

    let jh_block_emitter_task = {
        tokio::task::spawn(async move {
            // by blockhash
            let mut recent_confirmed_blocks = HashMap::<String, ProducedBlock>::new();
            let mut confirmed_blocks_stream = std::pin::pin!(confirmed_blocks_stream);
            let mut finalized_blockmeta_stream = std::pin::pin!(finalized_blockmeta_stream);

            let sender = producedblock_sender;
            let mut cleanup_tick = tokio::time::interval(Duration::from_secs(5));
            let mut current_slot: Slot = 0;
            loop {
                tokio::select! {
                    confirmed_block = confirmed_blocks_stream.next() => {
                        let confirmed_block = confirmed_block.expect("confirmed block from stream");
                        current_slot = confirmed_block.slot;
                        trace!("got confirmed block {} with blockhash {}",
                            confirmed_block.slot, confirmed_block.blockhash.clone());
                        if let Err(e) = sender.send(confirmed_block.clone()) {
                            panic!("Confirmed block stream send gave error {e:?}");
                        }
                        recent_confirmed_blocks.insert(confirmed_block.blockhash.clone(), confirmed_block);
                    },
                    meta_finalized = finalized_blockmeta_stream.next() => {
                        let blockhash = meta_finalized.expect("finalized block meta from stream");
                        if let Some(cached_confirmed_block) = recent_confirmed_blocks.remove(&blockhash) {
                            let finalized_block = cached_confirmed_block.to_finalized_block();
                            debug!("got finalized blockmeta {} with blockhash {}",
                                finalized_block.slot, finalized_block.blockhash.clone());
                            if let Err(e) = sender.send(finalized_block) {
                                panic!("Finalized block stream send gave error {e:?}");
                            }
                        } else {
                            debug!("finalized block meta received for blockhash {} which was never seen or already emitted", blockhash);
                        }
                    },
                    _ = cleanup_tick.tick() => {
                        let size_before = recent_confirmed_blocks.len();
                        recent_confirmed_blocks.retain(|_blockhash, block| {
                            block.slot > current_slot - 100 // must be greater than finalized slot distance (31)
                        });
                        let cleaned = size_before - recent_confirmed_blocks.len();
                        if cleaned > 0 {
                            debug!("cleaned {} confirmed blocks from cache", cleaned);
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
                commitment: Some(yellowstone_grpc_proto::geyser::CommitmentLevel::Processed as i32),
                accounts_data_slice: Default::default(),
                ping: None,
            };

            let stream = create_geyser_reconnecting_stream(grpc_source.clone(), filter);
            streams.push(stream);
        }

        create_multiplexed_stream(streams, SlotExtractor {})
    };

    let (multiplexed_stream, jh_channelizer) = channelize_stream(multiplex_stream);

    (multiplexed_stream, jh_channelizer)
}
