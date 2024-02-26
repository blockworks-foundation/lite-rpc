use std::path::Path;
use log::debug;
use solana_sdk::commitment_config::CommitmentConfig;
use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use crate::grpc_subscription::from_grpc_block_update;
use yellowstone_grpc_proto::prost::Message;

pub fn decode_from_dump(block_file: &Path) -> ProducedBlock {
    let block_bytes = std::fs::read(block_file).unwrap();
    debug!("read {} bytes from block file", block_bytes.len());

    let subscribe_update_block = SubscribeUpdateBlock::decode(block_bytes.as_slice()).expect("Block file must be protobuf");

    let produce_block = from_grpc_block_update(subscribe_update_block, CommitmentConfig::confirmed());

    produce_block
}