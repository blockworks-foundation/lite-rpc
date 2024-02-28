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
    use crate::grpc_subscription::from_grpc_block_update;

    #[test]
    fn map_block() {
        // version yellowstone.1.12+solana.1.17.15
        let raw_block = include_bytes!("block-000251402816-confirmed-1707315774189.dat");

        let example_block = SubscribeUpdateBlock::decode(raw_block.as_slice()).expect("Block file must be protobuf");
        // info!("example_block: {:?}", example_block);

        let started_at = Instant::now();
        let _produced_block = from_grpc_block_update(example_block, CommitmentConfig::confirmed());
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
        let produced_block = from_grpc_block_update(example_block, CommitmentConfig::confirmed());
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

fn maptx(tx: SubscribeUpdateTransactionInfo) -> Option<TransactionInfo> {

    let log_timer_tx = crate::grpc_subscription::LoggingTimer { started_at: Instant::now(), threshold: Duration::from_millis(10) };
    log_timer_tx.log_if_exceed("start");
    let meta = tx.meta?;

    let transaction = tx.transaction?;

    let message = transaction.message?;

    let header = message.header?;

    let signatures = transaction
        .signatures
        .into_iter()
        .filter_map(|sig| match Signature::try_from(sig) {
            Ok(sig) => Some(sig),
            Err(_) => {
                log::warn!(
                            "Failed to read signature from transaction in block TBD - skipping",
                            // block.blockhash // TODO
                        );
                None
            }
        })
        .collect_vec();
    log_timer_tx.log_if_exceed("after signatures");

    let err = meta.err.map(|x| {
        bincode::deserialize::<TransactionError>(&x.err)
            .expect("TransactionError should be deserialized")
    });
    log_timer_tx.log_if_exceed("after err");

    let signature = signatures[0];
    let compute_units_consumed = meta.compute_units_consumed;
    let account_keys: Vec<Pubkey> = message
        .account_keys
        .into_iter()
        .map(|key| {
            let bytes: [u8; 32] = key.try_into().unwrap_or(Pubkey::default().to_bytes());
            Pubkey::new_from_array(bytes)
        })
        .collect();
    log_timer_tx.log_if_exceed("after account keys"); // 47us

    let message = VersionedMessage::V0(v0::Message {
        header: MessageHeader {
            num_required_signatures: header.num_required_signatures as u8,
            num_readonly_signed_accounts: header.num_readonly_signed_accounts as u8,
            num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u8,
        },
        account_keys: account_keys.clone(),
        recent_blockhash: Hash::new(&message.recent_blockhash),
        instructions: message
            .instructions
            .into_iter()
            .map(|ix| CompiledInstruction {
                program_id_index: ix.program_id_index as u8,
                accounts: ix.accounts,
                data: ix.data,
            })
            .collect(),
        address_table_lookups: message
            .address_table_lookups
            .into_iter()
            .map(|table| {
                let bytes: [u8; 32] = table
                    .account_key
                    .try_into()
                    .unwrap_or(Pubkey::default().to_bytes());
                MessageAddressTableLookup {
                    account_key: Pubkey::new_from_array(bytes),
                    writable_indexes: table.writable_indexes,
                    readonly_indexes: table.readonly_indexes,
                }
            })
            .collect(),
    });
    log_timer_tx.log_if_exceed("after message mapping");

    let legacy_compute_budget: Option<(u32, Option<u64>)> =
        message.instructions().iter().find_map(|i| {
            if i.program_id(message.static_account_keys())
                .eq(&compute_budget::id())
            {
                if let Ok(ComputeBudgetInstruction::RequestUnitsDeprecated {
                              units,
                              additional_fee,
                          }) = try_from_slice_unchecked(i.data.as_slice())
                {
                    if additional_fee > 0 {
                        return Some((
                            units,
                            Some(((units * 1000) / additional_fee) as u64),
                        ));
                    } else {
                        return Some((units, None));
                    }
                }
            }
            None
        });
    log_timer_tx.log_if_exceed("after legacy_compute_budget");

    let legacy_cu_requested = legacy_compute_budget.map(|x| x.0);
    let legacy_prioritization_fees = legacy_compute_budget.map(|x| x.1).unwrap_or(None);

    let cu_requested = message
        .instructions()
        .iter()
        .find_map(|i| {
            if i.program_id(message.static_account_keys())
                .eq(&compute_budget::id())
            {
                if let Ok(ComputeBudgetInstruction::SetComputeUnitLimit(limit)) =
                    try_from_slice_unchecked(i.data.as_slice())
                {
                    return Some(limit);
                }
            }
            None
        })
        .or(legacy_cu_requested);
    log_timer_tx.log_if_exceed("after cu_requested");

    let prioritization_fees = message
        .instructions()
        .iter()
        .find_map(|i| {
            if i.program_id(message.static_account_keys())
                .eq(&compute_budget::id())
            {
                if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(price)) =
                    try_from_slice_unchecked(i.data.as_slice())
                {
                    return Some(price);
                }
            }

            None
        })
        .or(legacy_prioritization_fees);
    log_timer_tx.log_if_exceed("after prioritization_fees");

    let readable_accounts = account_keys
        .iter()
        .enumerate()
        .filter(|(index, _)| !message.is_maybe_writable(*index))
        .map(|(_, pk)| *pk)
        .collect();
    log_timer_tx.log_if_exceed("after readable_accounts"); // 80us



    let is_vote_transaction = message.instructions().iter().any(|i| {
        i.program_id(message.static_account_keys())
            .eq(&solana_sdk::vote::program::id())
            && limited_deserialize::<VoteInstruction>(&i.data)
            .map(|vi| vi.is_simple_vote())
            .unwrap_or(false)
    });
    log_timer_tx.log_if_exceed("after is_vote_transaction");

    let readable_accounts2: Vec<Pubkey> = account_keys
        .iter()
        .enumerate()
        .filter(|(index, _)| !message.is_maybe_writable(*index))
        .map(|(_, pk)| *pk)
        .collect();
    log_timer_tx.log_if_exceed("after readable_accounts2"); // 80us





    let writable_accounts = account_keys
        .iter()
        .enumerate()
        .filter(|(index, _)| message.is_maybe_writable(*index))
        .map(|(_, pk)| *pk)
        .collect();
    log_timer_tx.log_if_exceed("after writable_accounts");

    let address_lookup_tables = message
        .address_table_lookups()
        .map(|x| x.to_vec())
        .unwrap_or_default();
    log_timer_tx.log_if_exceed("after address_lookup_tables");


    // println!("elapsed: {:?} for tx", log_timer_tx.elapsed());
    log_timer_tx.log_if_exceed("before return");
    Some(TransactionInfo {
        signature: signature.to_string(),
        is_vote: is_vote_transaction,
        err,
        cu_requested,
        prioritization_fees,
        cu_consumed: compute_units_consumed,
        recent_blockhash: message.recent_blockhash().to_string(),
        message: BASE64.encode(message.serialize()),
        readable_accounts,
        writable_accounts,
        address_lookup_tables,
    })
}

