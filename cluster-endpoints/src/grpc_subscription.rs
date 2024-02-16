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

struct LoggingTimer {
    started_at: Instant,
    threshold: Duration,
}

#[allow(dead_code)]
impl LoggingTimer {
    fn log_if_exceed(&self, name: &str) {
        let elapsed = self.started_at.elapsed();
        if elapsed > self.threshold {
            eprintln!("{} exceeded: {:?}", name, elapsed);
        }
    }

    fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }
}

/// grpc version of ProducedBlock mapping
pub fn from_grpc_block_update(
    block: SubscribeUpdateBlock,
    commitment_config: CommitmentConfig,
) -> ProducedBlock {
    let _span = debug_span!("from_grpc_block_update", ?block.slot).entered();
    let started_at = Instant::now();
    let log_timer = LoggingTimer {
        started_at,
        threshold: Duration::from_millis(40),
    };

    log_timer.log_if_exceed("start");
    let txs: Vec<TransactionInfo> = block // TODO: rayon iter here
        .transactions
        .into_iter()
        .filter_map(|tx| maptx(tx))
        .collect();
    log_timer.log_if_exceed("after transactions");
    println!("tx count {}", txs.len());

    let rewards = block.rewards.map(|rewards| {
        rewards
            .rewards
            .into_iter()
            .map(|reward| Reward {
                pubkey: reward.pubkey.to_owned(),
                lamports: reward.lamports,
                post_balance: reward.post_balance,
                reward_type: match reward.reward_type() {
                    yellowstone_grpc_proto::prelude::RewardType::Unspecified => None,
                    yellowstone_grpc_proto::prelude::RewardType::Fee => Some(RewardType::Fee),
                    yellowstone_grpc_proto::prelude::RewardType::Rent => Some(RewardType::Rent),
                    yellowstone_grpc_proto::prelude::RewardType::Staking => {
                        Some(RewardType::Staking)
                    }
                    yellowstone_grpc_proto::prelude::RewardType::Voting => Some(RewardType::Voting),
                },
                commission: None,
            })
            .collect_vec()
    });
    log_timer.log_if_exceed("after rewards");

    let leader_id = if let Some(rewards) = &rewards {
        rewards
            .iter()
            .find(|reward| Some(RewardType::Fee) == reward.reward_type)
            .map(|leader_reward| leader_reward.pubkey.clone())
    } else {
        None
    };
    log_timer.log_if_exceed("after leader_id");

    log_timer.log_if_exceed("before return");
    ProducedBlock {
        transactions: txs,
        block_height: block
            .block_height
            .map(|block_height| block_height.block_height)
            .unwrap(),
        block_time: block.block_time.map(|time| time.timestamp).unwrap() as u64,
        blockhash: block.blockhash,
        previous_blockhash: block.parent_blockhash,
        commitment_config,
        leader_id,
        parent_slot: block.parent_slot,
        slot: block.slot,
        rewards,
    }
}

pub fn create_grpc_subscription(
    rpc_client: Arc<RpcClient>,
    grpc_sources: Vec<GrpcSourceConfig>,
    accounts_filter: AccountFilters,
) -> anyhow::Result<(EndpointStreaming, Vec<AnyhowJoinHandle>)> {
    let (cluster_info_sx, cluster_info_notifier) = tokio::sync::broadcast::channel(10);
    let (va_sx, vote_account_notifier) = tokio::sync::broadcast::channel(10);

    // processed slot is required to keep up with leader schedule
    let (slot_multiplex_channel, jh_multiplex_slotstream) =
        create_grpc_multiplex_processed_slots_subscription(grpc_sources.clone());

    let (block_multiplex_channel, jh_multiplex_blockstream) =
        create_grpc_multiplex_blocks_subscription(grpc_sources.clone());

    let cluster_info_polling = poll_cluster_info(rpc_client.clone(), cluster_info_sx);
    let vote_accounts_polling = poll_vote_accounts(rpc_client.clone(), va_sx);

    // accounts
    if !accounts_filter.is_empty() {
        let (account_jh, processed_account_stream) =
            create_grpc_account_streaming(grpc_sources, accounts_filter);
        let streamers = EndpointStreaming {
            blocks_notifier: block_multiplex_channel,
            slot_notifier: slot_multiplex_channel,
            cluster_info_notifier,
            vote_account_notifier,
            processed_account_stream: Some(processed_account_stream),
        };

        let endpoint_tasks = vec![
            jh_multiplex_slotstream,
            jh_multiplex_blockstream,
            cluster_info_polling,
            vote_accounts_polling,
            account_jh,
        ];
        Ok((streamers, endpoint_tasks))
    } else {
        let streamers = EndpointStreaming {
            blocks_notifier: block_multiplex_channel,
            slot_notifier: slot_multiplex_channel,
            cluster_info_notifier,
            vote_account_notifier,
            processed_account_stream: None,
        };

        let endpoint_tasks = vec![
            jh_multiplex_slotstream,
            jh_multiplex_blockstream,
            cluster_info_polling,
            vote_accounts_polling,
        ];
        Ok((streamers, endpoint_tasks))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::deserialize;
    use std::str::FromStr;
    use std::time::Instant;
    use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;
    use yellowstone_grpc_proto::prost::Message;

    #[test]
    fn map_block() {
        // version yellowstone.1.12+solana.1.17.15
        let raw_block = include_bytes!("block-000251402816-confirmed-1707315774189.dat");

        let example_block = SubscribeUpdateBlock::decode(raw_block.as_slice())
            .expect("Block file must be protobuf");
        // info!("example_block: {:?}", example_block);

        let started_at = Instant::now();
        let _produced_block = from_grpc_block_update(example_block, CommitmentConfig::confirmed());
        println!(
            "from_grpc_block_update mapping took: {:?}",
            started_at.elapsed()
        );
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
        let example_block = SubscribeUpdateBlock::decode(raw_block.as_slice())
            .expect("Block file must be protobuf");
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
    let log_timer_tx = LoggingTimer {
        started_at: Instant::now(),
        threshold: Duration::from_micros(10),
    };
    log_timer_tx.log_if_exceed("start");
    let meta = tx.meta?;

    let message = {
        let transaction = tx.transaction?;

        let grpc_message = transaction.message?;
        let header = grpc_message.header?;

        let account_keys: Vec<Pubkey> = grpc_message
            .account_keys
            .into_iter()
            .map(|key| {
                let bytes: [u8; 32] = key.try_into().unwrap_or(Pubkey::default().to_bytes());
                Pubkey::new_from_array(bytes)
            })
            .collect();
        log_timer_tx.log_if_exceed("after account keys"); // 47us

        let address_table_lookups: Vec<MessageAddressTableLookup> = grpc_message
            .address_table_lookups
            .into_iter()
            .map(|table| {
                let bytes = table
                    .account_key
                    .as_slice()
                    .try_into()
                    .unwrap_or(Pubkey::default().to_bytes());
                MessageAddressTableLookup {
                    account_key: Pubkey::from(bytes),
                    writable_indexes: table.writable_indexes,
                    readonly_indexes: table.readonly_indexes,
                }
            })
            .collect();
        log_timer_tx.log_if_exceed("after address_lookup_tables");

        VersionedMessage::V0(v0::Message {
            header: MessageHeader {
                num_required_signatures: header.num_required_signatures as u8,
                num_readonly_signed_accounts: header.num_readonly_signed_accounts as u8,
                num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u8,
            },
            account_keys: account_keys,
            recent_blockhash: Hash::new(&grpc_message.recent_blockhash),
            instructions: grpc_message
                .instructions
                .into_iter()
                .map(|ix| CompiledInstruction {
                    program_id_index: ix.program_id_index as u8,
                    accounts: ix.accounts,
                    data: ix.data,
                })
                .collect(),
            address_table_lookups,
        })
    };
    log_timer_tx.log_if_exceed("after message mapping");

    let sig_bytes: [u8; 64] = tx.signature.try_into().unwrap();
    let signature = Signature::from(sig_bytes);
    log_timer_tx.log_if_exceed("after signature");

    let err = meta.err.map(|x| {
        bincode::deserialize::<TransactionError>(&x.err)
            .expect("TransactionError should be deserialized")
    });
    log_timer_tx.log_if_exceed("after err");

    // iter message instructions
    let mut cu_requested: Option<u32> = None;
    let mut legacy_cu_requested: Option<u32> = None;
    let mut prioritization_fees: Option<u64> = None;
    let mut legacy_prioritization_fees: Option<u64> = None;
    let mut is_vote_transaction = false;

    for ix in message.instructions().iter() {
        // check vote
        if !is_vote_transaction
            && ix
                .program_id(message.static_account_keys())
                .eq(&solana_sdk::vote::program::id())
        {
            if let Ok(_) = limited_deserialize::<VoteInstruction>(&ix.data) {
                // should be able to just read bytes here
                is_vote_transaction = true;
                continue;
            }
        }

        // compute budget ixs
        if ix
            .program_id(message.static_account_keys())
            .eq(&compute_budget::id())
        {
            // cu_requested
            if let Ok(ComputeBudgetInstruction::SetComputeUnitLimit(limit)) =
                try_from_slice_unchecked(ix.data.as_slice())
            {
                cu_requested = Some(limit);
                continue;
            }

            // prio fees
            if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(price)) =
                try_from_slice_unchecked(ix.data.as_slice())
            {
                prioritization_fees = Some(price);
            }

            // legacy
            if let Ok(ComputeBudgetInstruction::RequestUnitsDeprecated {
                units,
                additional_fee,
            }) = try_from_slice_unchecked(ix.data.as_slice())
            {
                legacy_cu_requested = Some(units);
                if additional_fee > 0 {
                    legacy_prioritization_fees = Some((units * 1000 / additional_fee) as u64);
                }
                continue;
            }
        }
    }
    log_timer_tx.log_if_exceed("after message ix mapping");

    // Accounts
    let mut readable_accounts = vec![];
    let mut writable_accounts = vec![];

    for (index, key) in message.static_account_keys().iter().enumerate() {
        if message.is_maybe_writable(index) {
            writable_accounts.push(*key)
        } else {
            readable_accounts.push(*key)
        }
    }

    log_timer_tx.log_if_exceed("after account_keys");

    // println!("elapsed: {:?} for tx", log_timer_tx.elapsed());
    log_timer_tx.log_if_exceed("before return");
    Some(TransactionInfo {
        signature: signature.to_string(),
        is_vote: is_vote_transaction,
        err,
        cu_requested: cu_requested.or(legacy_cu_requested),
        prioritization_fees: prioritization_fees.or(legacy_prioritization_fees),
        cu_consumed: meta.compute_units_consumed,
        recent_blockhash: message.recent_blockhash().to_string(),
        message: BASE64.encode(message.serialize()),
        readable_accounts,
        writable_accounts,
        address_lookup_tables: message.address_table_lookups().unwrap().to_vec(),
    })
}
