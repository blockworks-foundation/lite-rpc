use crate::endpoint_stremers::EndpointStreaming;
use crate::grpc::gprc_accounts_streaming::create_grpc_account_streaming;
use crate::grpc_multiplex::{
    create_grpc_multiplex_blocks_subscription, create_grpc_multiplex_processed_slots_subscription,
};
use geyser_grpc_connector::GrpcSourceConfig;
use itertools::Itertools;
use log::{info, trace};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::structures::account_filter::AccountFilters;
use solana_lite_rpc_core::{
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
use solana_transaction_status::{InnerInstruction, InnerInstructions, Reward, RewardType, TransactionStatusMeta};
use std::cell::OnceCell;
use std::sync::Arc;
use solana_sdk::message::legacy;
use tracing::trace_span;

use crate::rpc_polling::vote_accounts_and_cluster_info_polling::{
    poll_cluster_info, poll_vote_accounts,
};
use solana_lite_rpc_core::solana_utils::hash_from_str;
use solana_lite_rpc_core::structures::produced_block::ProducedBlockInner;
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;

/// grpc version of ProducedBlock mapping
pub fn from_grpc_block_update(
    block: SubscribeUpdateBlock,
    commitment_config: CommitmentConfig,
) -> ProducedBlock {
    let num_transactions = block.transactions.len();
    let _span = trace_span!("from_grpc_block_update", ?block.slot, ?num_transactions).entered();
    let txs: Vec<TransactionInfo> = block
        .transactions
        .into_iter()
        .sorted_by_key(|tx| tx.index)
        .filter_map(|tx| {
            let meta = tx.meta?;

            let transaction = tx.transaction?;

            let message = transaction.message?;

            let header = message.header?;

            let signature = {
                let sig_bytes: [u8; 64] = tx.signature.try_into().expect("must map to signature");
                Signature::from(sig_bytes)
            };

            let err = meta.err.map(|x| {
                bincode::deserialize::<TransactionError>(&x.err)
                    .expect("TransactionError should be deserialized")
            });

            let inner_instructions: Vec<InnerInstructions> =
                meta.inner_instructions.into_iter().map(|yellow| {
                    InnerInstructions {
                        index: yellow.index as u8,
                        instructions: yellow.instructions.into_iter().map(|ins| {
                            InnerInstruction {
                                instruction: CompiledInstruction {
                                    program_id_index: ins.program_id_index as u8,
                                    accounts: ins.accounts,
                                    data: ins.data,
                                },
                                stack_height: ins.stack_height,
                            }
                        }).collect(),
                    }
                }).collect_vec();

            let compute_units_consumed = meta.compute_units_consumed;
            let account_keys: Vec<Pubkey> = message
                .account_keys
                .into_iter()
                .map(|key_bytes| {
                    let slice: &[u8] = key_bytes.as_slice();
                    Pubkey::try_from(slice).expect("must map to pubkey")
                })
                .collect();

            // use V0 as container and map to Legacy if needed
            let full_message = v0::Message {
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
                        let slice: &[u8] = table.account_key.as_slice();
                        let account_key = Pubkey::try_from(slice).expect("must map to pubkey");
                        MessageAddressTableLookup {
                            account_key,
                            writable_indexes: table.writable_indexes,
                            readonly_indexes: table.readonly_indexes,
                        }
                    })
                    .collect(),
            };

            let message = map_versioned_message(message.versioned, full_message);

            let (cu_requested, prioritization_fees) = map_compute_budget_instructions(&message);

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

            let log_messages = if meta.log_messages_none {
                None
            } else {
                Some(meta.log_messages)
            };

            Some(TransactionInfo {
                signature,
                index: tx.index as i32,
                is_vote: is_vote_transaction,
                err,
                cu_requested,
                prioritization_fees,
                cu_consumed: compute_units_consumed,
                recent_blockhash: *message.recent_blockhash(),
                message,
                readable_accounts,
                writable_accounts,
                address_lookup_tables,
                fee: meta.fee as i64,
                pre_balances: meta.pre_balances.into_iter().map(|x| x as i64).collect(),
                post_balances: meta.post_balances.into_iter().map(|x| x as i64).collect(),
                inner_instructions: Option::from(inner_instructions),
                log_messages,
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
        blockhash: hash_from_str(&block.blockhash).expect("valid blockhash"),
        previous_blockhash: hash_from_str(&block.parent_blockhash).expect("valid blockhash"),
        leader_id,
        parent_slot: block.parent_slot,
        slot: block.slot,
        rewards,
    };
    ProducedBlock::new(inner, commitment_config)
}

fn map_versioned_message(versioned: bool, full_message: v0::Message) -> VersionedMessage {
    if versioned {
        VersionedMessage::V0(full_message)
    } else {
        VersionedMessage::Legacy(legacy::Message {
            header: full_message.header,
            account_keys: full_message.account_keys,
            recent_blockhash: full_message.recent_blockhash,
            instructions: full_message.instructions,
        })
    }
}

fn map_compute_budget_instructions(message: &VersionedMessage) -> (Option<u32>, Option<u64>) {
    let cu_requested_cell: OnceCell<u32> = OnceCell::new();
    let legacy_cu_requested_cell: OnceCell<u32> = OnceCell::new();

    let prioritization_fees_cell: OnceCell<u64> = OnceCell::new();
    let legacy_prio_fees_cell: OnceCell<u64> = OnceCell::new();

    for compute_budget_ins in message.instructions().iter().filter(|instruction| {
        instruction
            .program_id(message.static_account_keys())
            .eq(&compute_budget::id())
    }) {
        if let Ok(budget_ins) =
            try_from_slice_unchecked::<ComputeBudgetInstruction>(compute_budget_ins.data.as_slice())
        {
            match budget_ins {
                // aka cu requested
                ComputeBudgetInstruction::SetComputeUnitLimit(limit) => {
                    cu_requested_cell
                        .set(limit)
                        .expect("cu_limit must be set only once");
                }
                // aka prio fees
                ComputeBudgetInstruction::SetComputeUnitPrice(price) => {
                    prioritization_fees_cell
                        .set(price)
                        .expect("prioritization_fees must be set only once");
                }
                // legacy
                ComputeBudgetInstruction::RequestUnitsDeprecated {
                    units,
                    additional_fee,
                } => {
                    let _ = legacy_cu_requested_cell.set(units);
                    if additional_fee > 0 {
                        let _ = legacy_prio_fees_cell.set(((units * 1000) / additional_fee) as u64);
                    };
                }
                _ => {
                    trace!("skip compute budget instruction");
                }
            }
        }
    }

    let cu_requested = cu_requested_cell
        .get()
        .or(legacy_cu_requested_cell.get())
        .cloned();
    let prioritization_fees = prioritization_fees_cell
        .get()
        .or(legacy_prio_fees_cell.get())
        .cloned();
    (cu_requested, prioritization_fees)
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
