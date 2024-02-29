use crate::endpoint_stremers::EndpointStreaming;
use crate::grpc::gprc_accounts_streaming::create_grpc_account_streaming;
use crate::grpc_multiplex::{
    create_grpc_multiplex_blocks_subscription, create_grpc_multiplex_processed_slots_subscription,
};
use geyser_grpc_connector::GrpcSourceConfig;
use itertools::Itertools;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::structures::account_filter::AccountFilters;
use solana_lite_rpc_core::{
    encoding::BASE64,
    structures::produced_block::{ProducedBlock, TransactionInfo},
    AnyhowJoinHandle,
};
use solana_sdk::program_utils::limited_deserialize;
use solana_sdk::vote::instruction::VoteInstruction;
use solana_sdk::{
    borsh0_10::try_from_slice_unchecked,
    commitment_config::CommitmentConfig,
    compute_budget::{self, ComputeBudgetInstruction},
    hash::Hash,
    instruction::CompiledInstruction,
    message::{
        v0::{self, MessageAddressTableLookup},
        MessageHeader, VersionedMessage,
    },
    pubkey::Pubkey,
    signature::Signature,
    transaction::TransactionError,
};
use solana_transaction_status::{Reward, RewardType};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::debug_span;
use yellowstone_grpc_proto::geyser::SubscribeUpdateTransactionInfo;

use crate::rpc_polling::vote_accounts_and_cluster_info_polling::{
    poll_cluster_info, poll_vote_accounts,
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;



#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;
    use yellowstone_grpc_proto::prost::Message;
    use std::str::FromStr;
    use bincode::deserialize;
    use crate::grpc_subscription::from_grpc_block_update_old;

    #[test]
    fn map_block() {
        // version yellowstone.1.12+solana.1.17.15
        let raw_block = include_bytes!("block-000251402816-confirmed-1707315774189.dat");

        let example_block = SubscribeUpdateBlock::decode(raw_block.as_slice()).expect("Block file must be protobuf");
        // info!("example_block: {:?}", example_block);

        let started_at = Instant::now();
        let _produced_block = from_grpc_block_update_old(example_block, CommitmentConfig::confirmed());
        println!("from_grpc_block_update mapping took: {:?}", started_at.elapsed());
    }

    #[test]
    fn mappaccount() {
        // account: tKeYE4wtowRb8yRroZShTipE18YVnqwXjsSAoNsFU6g
        // account: ComputeBudget111111111111111111111111111111
        // account: MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr
        let key1 = Pubkey::from_str("tKeYE4wtowRb8yRroZShTipE18YVnqwXjsSAoNsFU6g").unwrap();
        let key2 = Pubkey::from_str("ComputeBudget111111111111111111111111111111").unwrap();
        let key3 = Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr").unwrap();

        let raw_block = include_bytes!("block-000251402816-confirmed-1707315774189.dat");
        let example_block = SubscribeUpdateBlock::decode(raw_block.as_slice()).expect("Block file must be protobuf");
        let produced_block = from_grpc_block_update_old(example_block, CommitmentConfig::confirmed());
        let raw = &produced_block.transactions[0].message;
        // BASE64.encode(message.serialize())
        let vec: Vec<u8> = BASE64.decode(raw).unwrap();
        let message: VersionedMessage = deserialize::<VersionedMessage>(&vec).unwrap();

        let started_at = Instant::now();
        let _readable_accounts: Vec<Pubkey> = vec![key1, key2, key3]
            .iter()
            .enumerate()
            .filter(|(index, _)| !message.is_maybe_writable(*index))
            .map(|(_, pk)| *pk)
            .collect();
        println!("elapsed: {:?}", started_at.elapsed());
    }
}
