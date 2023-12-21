use std::collections::HashMap;
use crate::grpc_stream_utils::channelize_stream;
use crate::grpc_subscription::map_block_update;
use geyser_grpc_connector::grpc_subscription_autoreconnect::{create_geyser_reconnecting_stream, GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig, Message};
use geyser_grpc_connector::grpcmultiplex_fastestwins::{
    create_multiplexed_stream, FromYellowstoneExtractor,
};
use log::info;
use merge_streams::MergeStreams;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use std::time::Duration;
use futures::Stream;
use itertools::Itertools;
use solana_sdk::commitment_config;
use tokio::sync::broadcast::Receiver;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterSlots, SubscribeUpdate};
use solana_lite_rpc_core::structures::slot_notification::SlotNotification;

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

    let timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
    };

    let multiplex_stream_confirmed = {
        let commitment_config = CommitmentConfig::confirmed();

        let mut streams = Vec::new();
        for grpc_source in &grpc_sources {
            let stream =
                create_geyser_reconnecting_stream(
                    grpc_source.clone(),
                    GeyserFilter(commitment_config).blocks_and_txs(),
                );
            streams.push(stream);
        }

        create_multiplexed_stream(streams, BlockExtractor(commitment_config))
    };

    let multiplex_stream_finalized = {
        let commitment_config = CommitmentConfig::finalized();

        let mut streams = Vec::new();
        for grpc_source in &grpc_sources {
            let stream =
                create_geyser_reconnecting_stream(
                    grpc_source.clone(),
                    GeyserFilter(commitment_config).blocks_and_txs(),
                );
            streams.push(stream);
        }

        create_multiplexed_stream(streams, BlockExtractor(commitment_config))
    };

    let merged_stream_confirmed_finalize =
        (multiplex_stream_confirmed, multiplex_stream_finalized).merge();

    let (multiplexed_finalized_blocks, jh_channelizer) =
        channelize_stream(merged_stream_confirmed_finalize);

    (multiplexed_finalized_blocks, jh_channelizer)
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
                slots: slots,
                accounts: Default::default(),
                transactions: HashMap::new(),
                entry: Default::default(),
                blocks: HashMap::new(),
                blocks_meta: HashMap::new(),
                commitment: Some(yellowstone_grpc_proto::geyser::CommitmentLevel::Processed as i32),
                accounts_data_slice: Default::default(),
                ping: None,
            };

            let stream =
                create_geyser_reconnecting_stream(
                    grpc_source.clone(),
                    filter,
                );
            streams.push(stream);
        }

        create_multiplexed_stream(streams, SlotExtractor{})
    };

    let (multiplexed_stream, jh_channelizer) =
        channelize_stream(multiplex_stream);

    (multiplexed_stream, jh_channelizer)
}

