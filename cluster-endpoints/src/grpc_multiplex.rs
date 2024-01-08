use crate::grpc_stream_utils::channelize_stream;
use crate::grpc_subscription::map_block_update;
use futures::StreamExt;
use geyser_grpc_connector::grpc_subscription_autoreconnect::{
    create_geyser_reconnecting_stream, GeyserFilter, GrpcConnectionTimeouts, GrpcSourceConfig,
};
use geyser_grpc_connector::grpcmultiplex_fastestwins::{
    create_multiplexed_stream, FromYellowstoneExtractor,
};
use log::info;
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

struct BlockHashExtractor(CommitmentConfig);

impl FromYellowstoneExtractor for BlockHashExtractor {
    type Target = String;
    fn map_yellowstone_update(&self, update: SubscribeUpdate) -> Option<(u64, String)> {
        match update.update_oneof {
            Some(UpdateOneof::Block(block)) => Some((block.slot, block.blockhash)),
            Some(UpdateOneof::BlockMeta(block_meta)) => {
                Some((block_meta.slot, block_meta.blockhash))
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

    let _timeouts = GrpcConnectionTimeouts {
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        subscribe_timeout: Duration::from_secs(5),
    };

    let multiplex_stream_confirmed = {
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

    let (sender, multiplexed_merged_blocks) =
        tokio::sync::broadcast::channel::<ProducedBlock>(1000);

    let meta_stream_finalized = {
        let commitment_config = CommitmentConfig::finalized();

        let mut streams = Vec::new();
        for grpc_source in &grpc_sources {
            let stream = create_geyser_reconnecting_stream(
                grpc_source.clone(),
                GeyserFilter(commitment_config).blocks_meta(),
            );
            streams.push(stream);
        }
        create_multiplexed_stream(streams, BlockHashExtractor(commitment_config))
    };
    let jh_channelizer = {
        // spawn merged
        tokio::task::spawn(async move {
            let mut map_of_confimed_blocks = HashMap::<String, ProducedBlock>::new();
            let mut multiplex_stream_confirmed = std::pin::pin!(multiplex_stream_confirmed);
            let mut meta_stream_finalized = std::pin::pin!(meta_stream_finalized);
            let sender = sender;
            loop {
                tokio::select! {
                    confirmed_block = multiplex_stream_confirmed.next() => {
                        if let Some(confirmed_block) = confirmed_block {
                            if let Err(e) = sender.send(confirmed_block.clone()) {
                                panic!("Confirmed block stream send gave error {e:?}");
                            }
                            map_of_confimed_blocks.insert(confirmed_block.blockhash.clone(), confirmed_block);
                        } else {
                            panic!("Confirmed stream broke");
                        }
                    },
                    meta_finalized = meta_stream_finalized.next() => {
                        if let Some(blockhash) = meta_finalized {
                            if let Some(mut finalized_block) = map_of_confimed_blocks.remove(&blockhash) {
                                finalized_block.commitment_config = CommitmentConfig::finalized();
                                if let Err(e) = sender.send(finalized_block.clone()) {
                                    panic!("Finalized block stream send gave error {e:?}");
                                }
                            }
                        }
                    }
                }
            }
        })
    };

    (multiplexed_merged_blocks, jh_channelizer)
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
