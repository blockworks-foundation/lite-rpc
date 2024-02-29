use std::cell::OnceCell;
use std::io::Error;
use std::num::{NonZeroU32, NonZeroU64};
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
use log::{debug, info, trace};
use tracing::{debug_span, trace_span};
use yellowstone_grpc_proto::geyser::SubscribeUpdateTransactionInfo;

use crate::rpc_polling::vote_accounts_and_cluster_info_polling::{
    poll_cluster_info, poll_vote_accounts,
};
use yellowstone_grpc_proto::prelude::{Rewards, SubscribeUpdateBlock};
use solana_lite_rpc_core::structures::produced_block::ProducedBlockInner;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;

pub struct LoggingTimer {
    pub(crate) started_at: Instant,
    pub(crate) threshold: Duration,
}

#[allow(dead_code)]
impl LoggingTimer {

    pub fn log_if_exceed(&self, name: &str) {
        let elapsed = self.started_at.elapsed();
        if elapsed > self.threshold {
            eprintln!("{} exceeded: {:?}", name, elapsed);
        }

    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }
}



/// grpc version of ProducedBlock mapping
/// from_grpc_block_update_reimplement{block.slot=255548168 num_transactions=6171}: solana_lite_rpc_cluster_endpoints::grpc_subscription: close time.busy=37.9ms time.idle=5.21µs
/// from_grpc_block_update_reimplement{block.slot=255548169 num_transactions=529}: solana_lite_rpc_cluster_endpoints::grpc_subscription: close time.busy=4.84ms time.idle=4.46µs
pub fn from_grpc_block_update_reimplement(
    block: SubscribeUpdateBlock,
    commitment_config: CommitmentConfig,
) -> ProducedBlock {
    let num_transactions = block.transactions.len();
    let _span = debug_span!("from_grpc_block_update_reimplement", ?block.slot, ?num_transactions).entered();
    let started_at = Instant::now();

    log_timer.log_if_exceed("start");
    let txs: Vec<TransactionInfo> = block
        .transactions
        // .into_iter()
        .into_par_iter()
        .filter_map(|tx| maptx_reimplemented(tx))
        .collect();
    log_timer.log_if_exceed("after transactions");

    let (rewards, leader_id) = if let Some(rewards) = block.rewards {
        let (rewards, leader_id_opt) = map_rewards(&rewards);
        (Some(rewards), leader_id_opt)
    } else {
        (None, None)
    };

    log_timer.log_if_exceed("after leader_id");

    log_timer.log_if_exceed("before return");
    let inner = ProducedBlockInner {
        transactions: txs,
        block_height: block
            .block_height
            .map(|block_height| block_height.block_height)
            .unwrap(),
        block_time: block.block_time.map(|time| time.timestamp).unwrap() as u64,
        blockhash: block.blockhash,
        previous_blockhash: block.parent_blockhash,
        leader_id,
        parent_slot: block.parent_slot,
        slot: block.slot,
        rewards,
    };
    ProducedBlock::new(inner, commitment_config)
}

fn map_rewards(in_rewards: &Rewards) -> (Vec<Reward>, Option<String>) {
    let mut out_rewards = Vec::with_capacity(in_rewards.rewards.len());
    let leader_id = OnceCell::new();
    for reward in &in_rewards.rewards {
        let reward_type = match reward.reward_type() {
            yellowstone_grpc_proto::prelude::RewardType::Unspecified => None,
            yellowstone_grpc_proto::prelude::RewardType::Fee => Some(RewardType::Fee),
            yellowstone_grpc_proto::prelude::RewardType::Rent => Some(RewardType::Rent),
            yellowstone_grpc_proto::prelude::RewardType::Staking => {
                Some(RewardType::Staking)
            }
            yellowstone_grpc_proto::prelude::RewardType::Voting => Some(RewardType::Voting),
        };
        // check if leader
        if reward_type == Some(RewardType::Fee) {
            leader_id.set(reward.pubkey.clone()).expect("only one fee reward expected");
        }
        let r = Reward {
            pubkey: reward.pubkey.to_owned(),
            lamports: reward.lamports,
            post_balance: reward.post_balance,
            reward_type: reward_type,
            commission: None,
        };
        out_rewards.push(r);

    }
    (out_rewards, leader_id.into_inner())
}

pub fn from_grpc_block_update_optimized(
    block: SubscribeUpdateBlock,
    commitment_config: CommitmentConfig,
) -> ProducedBlock {
    let _span = debug_span!("from_grpc_block_update_optimized", ?block.slot).entered();
    let started_at = Instant::now();
    let log_timer = LoggingTimer { started_at, threshold: Duration::from_millis(40)};

    log_timer.log_if_exceed("start");
    let txs: Vec<TransactionInfo> = block
        .transactions
        .into_iter()
        // .into_par_iter() TODO
        .filter_map(|tx| maptxoptimized(tx))
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
    let inner = ProducedBlockInner {
        transactions: txs,
        block_height: block
            .block_height
            .map(|block_height| block_height.block_height)
            .unwrap(),
        block_time: block.block_time.map(|time| time.timestamp).unwrap() as u64,
        blockhash: block.blockhash,
        previous_blockhash: block.parent_blockhash,
        leader_id,
        parent_slot: block.parent_slot,
        slot: block.slot,
        rewards,
    };
    ProducedBlock::new(inner, commitment_config)
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


pub fn from_grpc_block_update_original(
    block: SubscribeUpdateBlock,
    commitment_config: CommitmentConfig,
) -> ProducedBlock {
    let _span = debug_span!("from_grpc_block_update_original", ?block.slot).entered();
    let txs: Vec<TransactionInfo> = block
        .transactions
        .into_iter()
        .filter_map(|tx| {
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
                            "Failed to read signature from transaction in block {} - skipping",
                            block.blockhash
                        );
                        None
                    }
                })
                .collect_vec();

            let err = meta.err.map(|x| {
                bincode::deserialize::<TransactionError>(&x.err)
                    .expect("TransactionError should be deserialized")
            });

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

            let is_vote_transaction = message.instructions().iter().any(|i| {
                i.program_id(message.static_account_keys())
                    .eq(&solana_sdk::vote::program::id())
                    && limited_deserialize::<VoteInstruction>(&i.data)
                    .map(|vi| vi.is_simple_vote())
                    .unwrap_or(false)
            });

            let readable_accounts = account_keys
                .iter()
                .enumerate()
                .filter(|(index, _)| !message.is_maybe_writable(*index))
                .map(|(_, pk)| *pk)
                .collect();
            let writable_accounts = account_keys
                .iter()
                .enumerate()
                .filter(|(index, _)| message.is_maybe_writable(*index))
                .map(|(_, pk)| *pk)
                .collect();

            let address_lookup_tables = message
                .address_table_lookups()
                .map(|x| x.to_vec())
                .unwrap_or_default();

            Some(TransactionInfo {
                signature: signature.to_string(),
                is_vote: is_vote_transaction,
                err,
                cu_requested,
                prioritization_fees,
                cu_consumed: compute_units_consumed,
                recent_blockhash: message.recent_blockhash().to_string(),
                message,
                readable_accounts,
                writable_accounts,
                address_lookup_tables,
            })
        })
        .collect();

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

    let leader_id = if let Some(rewards) = &rewards {
        rewards
            .iter()
            .find(|reward| Some(RewardType::Fee) == reward.reward_type)
            .map(|leader_reward| leader_reward.pubkey.clone())
    } else {
        None
    };

    let inner = ProducedBlockInner {
        transactions: txs,
        block_height: block
            .block_height
            .map(|block_height| block_height.block_height)
            .unwrap(),
        block_time: block.block_time.map(|time| time.timestamp).unwrap() as u64,
        blockhash: block.blockhash,
        previous_blockhash: block.parent_blockhash,
        leader_id,
        parent_slot: block.parent_slot,
        slot: block.slot,
        rewards,
    };
    ProducedBlock::new(inner, commitment_config)
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use super::*;
    use std::time::Instant;
    use yellowstone_grpc_proto::geyser::SubscribeUpdateBlock;
    use yellowstone_grpc_proto::prost::Message;
    use std::str::FromStr;
    use bincode::deserialize;
    use chrono::format;
    use log::Level::Debug;

    #[test]
    fn map_block() {
        // version yellowstone.1.12+solana.1.17.15
        let raw_block = include_bytes!("block-000251402816-confirmed-1707315774189.dat");

        let example_block = SubscribeUpdateBlock::decode(raw_block.as_slice()).expect("Block file must be protobuf");
        // info!("example_block: {:?}", example_block);

        let started_at = Instant::now();
        let _produced_block = from_grpc_block_update_reimplement(example_block, CommitmentConfig::confirmed());
        println!("from_grpc_block_update mapping took: {:?}", started_at.elapsed());
    }


    #[test]
    fn compare_old_new() {
        // version yellowstone.1.12+solana.1.17.15
        let raw_block = include_bytes!("block-000251402816-confirmed-1707315774189.dat");

        let example_block1 = SubscribeUpdateBlock::decode(raw_block.as_slice()).expect("Block file must be protobuf");
        let example_block2 = SubscribeUpdateBlock::decode(raw_block.as_slice()).expect("Block file must be protobuf");
        // info!("example_block: {:?}", example_block);

        let started_at = Instant::now();
        println!("from_grpc_block_update mapping took: {:?}", started_at.elapsed());
        let produced_block_old = from_grpc_block_update_original(example_block1, CommitmentConfig::confirmed());
        let produced_block_new = from_grpc_block_update_reimplement(example_block2, CommitmentConfig::confirmed());


        if format!("{:?}",produced_block_old) != format!("{:?}", produced_block_new) {
            panic!("produced_block_old != produced_block_new");
        }
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
        let produced_block = from_grpc_block_update_reimplement(example_block, CommitmentConfig::confirmed());
        let message: &VersionedMessage = &produced_block.transactions[0].message;
        // BASE64.encode(message.serialize())
        // let vec: Vec<u8> = BASE64.decode(raw).unwrap();
        // let message: VersionedMessage = deserialize::<VersionedMessage>(&vec).unwrap();

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

fn maptx_reimplemented(tx: SubscribeUpdateTransactionInfo) -> Option<TransactionInfo> {

    let log_timer_tx = LoggingTimer { started_at: Instant::now(), threshold: Duration::from_millis(10) };
    log_timer_tx.log_if_exceed("start");
    let meta = tx.meta?;

    let transaction = tx.transaction?;

    let message = transaction.message?;

    let header = message.header?;

    log_timer_tx.log_if_exceed("after signatures");

    let err = meta.err.map(|x| {
        bincode::deserialize::<TransactionError>(&x.err)
            .expect("TransactionError should be deserialized")
    });
    log_timer_tx.log_if_exceed("after err");

    // let signature = signatures_old[0];
    let signature = {
        let sig_bytes: [u8; 64] = tx.signature.try_into().unwrap();
        let signature = Signature::from(sig_bytes);
        signature
    };

    let compute_units_consumed = meta.compute_units_consumed;
    let account_keys: Vec<Pubkey> = message
        .account_keys
        .iter()
        .map(|key_bytes| {
            // caution:this changed
            // let bytes: [u8; 32] = key.try_into().unwrap_or(Pubkey::default().to_bytes());
            // Pubkey::new_from_array(bytes)
            let slice: &[u8] = key_bytes.as_slice();
            Pubkey::try_from(slice).expect("must map to pubkey")
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

    // opt, opt
    let (cu_requested, prioritization_fees) = map_compute_budget_instructions(&message);

    let mut readable_accounts = Vec::with_capacity(account_keys.len());
    let mut writable_accounts = Vec::with_capacity(account_keys.len());

    // clone the vec and move the ownership instead of iterating over single elements
    for (index, account_key) in account_keys.clone().into_iter().enumerate() {
        if message.is_maybe_writable(index) {
            writable_accounts.push(account_key)
        } else {
            readable_accounts.push(account_key)
        }
    }

    let is_vote_transaction = message.instructions().iter().any(|i| {
        i.program_id(message.static_account_keys())
            .eq(&solana_sdk::vote::program::id())
            && limited_deserialize::<VoteInstruction>(&i.data)
            .map(|vi| vi.is_simple_vote())
            .unwrap_or(false)
    });
    log_timer_tx.log_if_exceed("after is_vote_transaction");

    let address_lookup_tables = message
        .address_table_lookups()
        .map(|x| x.to_vec())
        .unwrap_or_default();
    log_timer_tx.log_if_exceed("after address_lookup_tables");


    log_timer_tx.log_if_exceed("before return");
    Some(TransactionInfo {
        signature: signature.to_string(),
        is_vote: is_vote_transaction,
        err,
        cu_requested,
        prioritization_fees,
        cu_consumed: compute_units_consumed,
        recent_blockhash: message.recent_blockhash().to_string(),
        message,
        readable_accounts,
        writable_accounts,
        address_lookup_tables,
    })
}

// refactored using "extract method"
fn map_compute_budget_instructions(message: &VersionedMessage) -> (Option<u32>, Option<u64>) {
    let _span = trace_span!("map_compute_budget_instructions").entered();

    let legacy_cell: OnceCell<(Option<u32>, Option<u64>)> = OnceCell::new();
    let cu_requested_cell: OnceCell<Option<u32>> = OnceCell::new();
    let prioritization_fees_cell: OnceCell<Option<u64>> = OnceCell::new();

    for compute_budget_ins in message.instructions().iter()
        .filter(|instruction| {
            instruction.program_id(message.static_account_keys()).eq(&compute_budget::id())}) {

        if let Ok(cbi) = try_from_slice_unchecked::<ComputeBudgetInstruction>(compute_budget_ins.data.as_slice()) {
            match cbi {
                // aka cu requested
                ComputeBudgetInstruction::SetComputeUnitLimit(limit) => {
                    // note: not use if the exactly-once invariant holds
                    let _was_set = cu_requested_cell.set(Some(limit)).expect("cu_limit must be set only once");
                }
                // aka prio fees
                ComputeBudgetInstruction::SetComputeUnitPrice(price) => {
                    // note: not use if the exactly-once invariant holds
                    let _was_set = prioritization_fees_cell.set(Some(price));
                }
                ComputeBudgetInstruction::RequestUnitsDeprecated { units, additional_fee } => {
                    let _was_set = cu_requested_cell.set(Some(units));
                    if additional_fee > 0 {
                        let _was_set = prioritization_fees_cell.set(Some(((units * 1000) / additional_fee) as u64));
                    };
                }
                _ => {
                    trace!("skip instruction");
                }
            }

        }
    }

    let legacy = legacy_cell.into_inner();
    let cu_requested = cu_requested_cell.into_inner().unwrap_or(legacy.and_then(|x| x.0));
    let prioritization_fees = prioritization_fees_cell.into_inner().unwrap_or(legacy.and_then(|x| x.1));
    (cu_requested, prioritization_fees)
}


fn maptxoptimized(tx: SubscribeUpdateTransactionInfo) -> Option<TransactionInfo> {
    let log_timer_tx = LoggingTimer {
        started_at: Instant::now(),
        threshold: Duration::from_millis(10),
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
                // do we even need to deserialize here?
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
        address_lookup_tables: message.address_table_lookups().unwrap().to_vec(),
        message,
        readable_accounts,
        writable_accounts,
    })
}


