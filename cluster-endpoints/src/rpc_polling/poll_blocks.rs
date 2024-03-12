use anyhow::{bail, Context};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::solana_utils::hash_from_str;
use solana_lite_rpc_core::structures::produced_block::{ProducedBlockInner, TransactionInfo};
use solana_lite_rpc_core::{
    structures::{
        produced_block::ProducedBlock,
        slot_notification::{AtomicSlot, SlotNotification},
    },
    AnyhowJoinHandle,
};
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::borsh0_10::try_from_slice_unchecked;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::program_utils::limited_deserialize;
use solana_sdk::reward_type::RewardType;
use solana_sdk::vote::instruction::VoteInstruction;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget,
    slot_history::Slot,
};
use solana_transaction_status::option_serializer::OptionSerializer;
use solana_transaction_status::{
    TransactionDetails, UiConfirmedBlock, UiTransactionEncoding, UiTransactionStatusMeta,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::broadcast::{Receiver, Sender};

pub const NUM_PARALLEL_TASKS_DEFAULT: usize = 16;

pub async fn process_block(
    rpc_client: &RpcClient,
    slot: Slot,
    commitment_config: CommitmentConfig,
) -> Option<ProducedBlock> {
    let block = rpc_client
        .get_block_with_config(
            slot,
            RpcBlockConfig {
                transaction_details: Some(TransactionDetails::Full),
                commitment: Some(commitment_config),
                max_supported_transaction_version: Some(0),
                encoding: Some(UiTransactionEncoding::Base64),
                rewards: Some(true),
            },
        )
        .await;
    block
        .ok()
        .map(|block| from_ui_block(block, slot, commitment_config))
}

pub fn poll_block(
    rpc_client: Arc<RpcClient>,
    block_notification_sender: Sender<ProducedBlock>,
    slot_notification: Receiver<SlotNotification>,
    num_parallel_tasks: usize,
) -> Vec<AnyhowJoinHandle> {
    let mut tasks: Vec<AnyhowJoinHandle> = vec![];

    let recent_slot = AtomicSlot::default();
    let (slot_retry_queue_sx, mut slot_retry_queue_rx) = tokio::sync::mpsc::unbounded_channel();
    let (block_schedule_queue_sx, block_schedule_queue_rx) =
        async_channel::unbounded::<(Slot, CommitmentConfig)>();

    for _i in 0..num_parallel_tasks {
        let block_notification_sender = block_notification_sender.clone();
        let rpc_client = rpc_client.clone();
        let block_schedule_queue_rx = block_schedule_queue_rx.clone();
        let slot_retry_queue_sx = slot_retry_queue_sx.clone();
        let task: AnyhowJoinHandle = tokio::spawn(async move {
            loop {
                let (slot, commitment_config) = block_schedule_queue_rx
                    .recv()
                    .await
                    .context("Recv error on block channel")?;
                let processed_block =
                    process_block(rpc_client.as_ref(), slot, commitment_config).await;
                match processed_block {
                    Some(processed_block) => {
                        block_notification_sender
                            .send(processed_block)
                            .context("Processed block should be sent")?;
                        // schedule to get finalized commitment
                        if commitment_config.commitment != CommitmentLevel::Finalized {
                            let retry_at = tokio::time::Instant::now()
                                .checked_add(Duration::from_secs(2))
                                .unwrap();
                            slot_retry_queue_sx
                                .send(((slot, CommitmentConfig::finalized()), retry_at))
                                .context("Failed to reschedule fetch of finalized block")?;
                        }
                    }
                    None => {
                        let retry_at = tokio::time::Instant::now()
                            .checked_add(Duration::from_millis(10))
                            .unwrap();
                        slot_retry_queue_sx
                            .send(((slot, commitment_config), retry_at))
                            .context("should be able to rescheduled for replay")?;
                    }
                }
            }
        });
        tasks.push(task);
    }

    //let replay task
    {
        let recent_slot = recent_slot.clone();
        let block_schedule_queue_sx = block_schedule_queue_sx.clone();
        let replay_task: AnyhowJoinHandle = tokio::spawn(async move {
            while let Some(((slot, commitment_config), instant)) = slot_retry_queue_rx.recv().await
            {
                let recent_slot = recent_slot.load(std::sync::atomic::Ordering::Relaxed);
                // if slot is too old ignore
                if recent_slot.saturating_sub(slot) > 128 {
                    // slot too old to retry
                    // most probably its an empty slot
                    continue;
                }

                let now = tokio::time::Instant::now();
                if now < instant {
                    tokio::time::sleep_until(instant).await;
                }
                if block_schedule_queue_sx
                    .send((slot, commitment_config))
                    .await
                    .is_err()
                {
                    bail!("could not schedule replay for a slot")
                }
            }
            unreachable!();
        });
        tasks.push(replay_task)
    }

    //slot poller
    let slot_poller = tokio::spawn(async move {
        log::info!("block listener started");
        let current_slot = rpc_client
            .get_slot()
            .await
            .context("Should get current slot")?;
        recent_slot.store(current_slot, std::sync::atomic::Ordering::Relaxed);
        let mut slot_notification = slot_notification;
        loop {
            let SlotNotification {
                estimated_processed_slot,
                ..
            } = slot_notification
                .recv()
                .await
                .context("Should get slot notification")?;
            let last_slot = recent_slot.load(std::sync::atomic::Ordering::Relaxed);
            if last_slot < estimated_processed_slot {
                recent_slot.store(
                    estimated_processed_slot,
                    std::sync::atomic::Ordering::Relaxed,
                );
                for slot in last_slot + 1..estimated_processed_slot + 1 {
                    block_schedule_queue_sx
                        .send((slot, CommitmentConfig::confirmed()))
                        .await
                        .context("Should be able to schedule message")?;
                }
            }
        }
    });
    tasks.push(slot_poller);

    tasks
}

/// rpc version of ProducedBlock mapping
pub fn from_ui_block(
    block: UiConfirmedBlock,
    slot: Slot,
    commitment_config: CommitmentConfig,
) -> ProducedBlock {
    let block_height = block.block_height.unwrap_or_default();
    let txs = block.transactions.unwrap_or_default();

    let blockhash = hash_from_str(&block.blockhash).expect("valid blockhash");
    let previous_blockhash = hash_from_str(&block.previous_blockhash).expect("valid blockhash");
    let parent_slot = block.parent_slot;
    let rewards = block.rewards.clone();

    let txs = txs
        .into_iter()
        .enumerate()
        .filter_map(|(idx_in_block, tx)| {
            let Some(UiTransactionStatusMeta {
                err,
                compute_units_consumed,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages,
                pre_token_balances,
                post_token_balances,
                ..
            }) = tx.meta
            else {
                // ignoring transaction
                log::info!("Tx with no meta");
                return None;
            };

            let Some(tx) = tx.transaction.decode() else {
                // ignoring transaction
                log::info!("Tx could not be decoded");
                return None;
            };

            let signature = tx.signatures[0];
            let cu_consumed = match compute_units_consumed {
                OptionSerializer::Some(cu_consumed) => Some(cu_consumed),
                _ => None,
            };

            let legacy_compute_budget = tx.message.instructions().iter().find_map(|i| {
                if i.program_id(tx.message.static_account_keys())
                    .eq(&compute_budget::id())
                {
                    if let Ok(ComputeBudgetInstruction::RequestUnitsDeprecated {
                        units,
                        additional_fee,
                    }) = try_from_slice_unchecked(i.data.as_slice())
                    {
                        return Some((units, additional_fee));
                    }
                }
                None
            });

            let mut cu_requested = tx.message.instructions().iter().find_map(|i| {
                if i.program_id(tx.message.static_account_keys())
                    .eq(&compute_budget::id())
                {
                    if let Ok(ComputeBudgetInstruction::SetComputeUnitLimit(limit)) =
                        try_from_slice_unchecked(i.data.as_slice())
                    {
                        return Some(limit);
                    }
                }
                None
            });

            let mut prioritization_fees = tx.message.instructions().iter().find_map(|i| {
                if i.program_id(tx.message.static_account_keys())
                    .eq(&compute_budget::id())
                {
                    if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(price)) =
                        try_from_slice_unchecked(i.data.as_slice())
                    {
                        return Some(price);
                    }
                }

                None
            });

            if let Some((units, additional_fee)) = legacy_compute_budget {
                cu_requested = Some(units);
                if additional_fee > 0 {
                    prioritization_fees = Some(calc_prioritization_fees(units, additional_fee))
                }
            };

            let blockhash = tx.message.recent_blockhash();

            let is_vote_transaction = tx.message.instructions().iter().any(|i| {
                i.program_id(tx.message.static_account_keys())
                    .eq(&solana_sdk::vote::program::id())
                    && limited_deserialize::<VoteInstruction>(&i.data)
                        .map(|vi| vi.is_simple_vote())
                        .unwrap_or(false)
            });

            let accounts = tx.message.static_account_keys();
            let mut readable_accounts = vec![];
            let mut writable_accounts = vec![];
            for (index, account) in accounts.iter().enumerate() {
                if tx.message.is_maybe_writable(index) {
                    writable_accounts.push(*account);
                } else {
                    readable_accounts.push(*account);
                }
            }

            let address_lookup_tables = tx
                .message
                .address_table_lookups()
                .map(|x| x.to_vec())
                .unwrap_or_default();

            let log_messages = match log_messages {
                OptionSerializer::Some(log_messages) => Some(log_messages),
                _ => None,
            };

            let pre_token_balances = match pre_token_balances {
                OptionSerializer::Some(pre_token_balances) => pre_token_balances,
                _ => vec![],
            };

            let post_token_balances = match post_token_balances {
                OptionSerializer::Some(post_token_balances) => post_token_balances,
                _ => vec![],
            };

            Some(TransactionInfo {
                signature,
                // note: not sure if the index from RPC is compatible with that from yellowstone
                index: idx_in_block as i32,
                is_vote: is_vote_transaction,
                err,
                cu_requested,
                prioritization_fees,
                cu_consumed,
                recent_blockhash: *blockhash,
                message: tx.message,
                readable_accounts,
                writable_accounts,
                address_lookup_tables,
                fee: fee as i64,
                pre_balances: pre_balances.into_iter().map(|x| x as i64).collect(),
                post_balances: post_balances.into_iter().map(|x| x as i64).collect(),
                inner_instructions: None, // not implemented for RPC
                log_messages: log_messages,
                pre_token_balances,
                post_token_balances,
            })
        })
        .collect();

    let leader_id = if let Some(rewards) = block.rewards {
        rewards
            .iter()
            .find(|reward| Some(RewardType::Fee) == reward.reward_type)
            .map(|leader_reward| leader_reward.pubkey.clone())
    } else {
        None
    };

    let block_time = block.block_time.unwrap_or(0) as u64;

    let inner = ProducedBlockInner {
        transactions: txs,
        block_height,
        leader_id,
        blockhash,
        previous_blockhash,
        parent_slot,
        block_time,
        slot,
        rewards,
    };
    ProducedBlock::new(inner, commitment_config)
}

#[inline]
fn calc_prioritization_fees(units: u32, additional_fee: u32) -> u64 {
    (units as u64 * 1000) / additional_fee as u64
}

#[test]
fn overflow_u32() {
    // value high enough to overflow u32 if multiplied by 1000
    let units: u32 = 4_000_000_000;
    let additional_fee: u32 = 100;
    let prioritization_fees: u64 = calc_prioritization_fees(units, additional_fee);

    assert_eq!(40_000_000_000, prioritization_fees);
}
