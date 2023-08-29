use std::{sync::Arc, time::Duration};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::{structures::{processed_block::ProcessedBlock, transaction_info::TransactionInfo, slot_notification::SlotNotification}, AnyhowJoinHandle, AtomicSlot};
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::{slot_history::Slot, commitment_config::CommitmentConfig, compute_budget::{self, ComputeBudgetInstruction}, borsh::try_from_slice_unchecked};
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding, UiTransactionStatusMeta, option_serializer::OptionSerializer, RewardType};
use anyhow::{Context, bail};
use tokio::sync::broadcast::{Sender, Receiver};

pub async fn process_block(
    rpc_client: &RpcClient,
    slot: Slot,
    commitment_config: CommitmentConfig,
) -> Option<ProcessedBlock> {
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

    if block.is_err() {
        return None;
    }
    let block = block.unwrap();

    let Some(block_height) = block.block_height else {
        return None;
    };

    let Some(txs) = block.transactions else {
        return None;
     };

    let blockhash = block.blockhash;
    let parent_slot = block.parent_slot;

    let txs = txs.into_iter().filter_map(|tx| {
        let Some(UiTransactionStatusMeta { err, status, compute_units_consumed ,.. }) = tx.meta else {
            log::info!("Tx with no meta");
            return None;
        };

        let Some(tx) = tx.transaction.decode() else {
            log::info!("Tx could not be decoded");
            return None;
        };

        let signature = tx.signatures[0].to_string();
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
                prioritization_fees = Some(((units * 1000) / additional_fee).into())
            }
        };

        Some(TransactionInfo {
            signature,
            err,
            status,
            cu_requested,
            prioritization_fees,
            cu_consumed,
        })
    }).collect();

    let leader_id = if let Some(rewards) = block.rewards {
        rewards
            .iter()
            .find(|reward| Some(RewardType::Fee) == reward.reward_type)
            .map(|leader_reward| leader_reward.pubkey.clone())
    } else {
        None
    };

    let block_time = block.block_time.unwrap_or(0) as u64;

    Some(ProcessedBlock {
        txs,
        block_height,
        leader_id,
        blockhash,
        parent_slot,
        block_time,
        slot,
        commitment_config,
    })
}

pub fn poll_block(rpc_client: Arc<RpcClient>, block_notification_sender: Sender<ProcessedBlock>, slot_notification: Receiver<SlotNotification>) -> Vec<AnyhowJoinHandle> {
    let mut tasks: Vec<AnyhowJoinHandle> = vec![];

    let recent_slot = AtomicSlot::default();
    let (slot_retry_queue_sx, mut slot_retry_queue_rx) = tokio::sync::mpsc::unbounded_channel();
    let (block_schedule_queue_sx, block_schedule_queue_rx) = async_channel::unbounded::<(Slot, CommitmentConfig)>();

    for _i in 0..8 {
        let block_notification_sender = block_notification_sender.clone();
        let rpc_client = rpc_client.clone();
        let block_schedule_queue_rx = block_schedule_queue_rx.clone();
        let slot_retry_queue_sx = slot_retry_queue_sx.clone();
        let task: AnyhowJoinHandle = tokio::spawn( async move {
            loop {
                let (slot, commitment_config) = block_schedule_queue_rx.recv().await.context("Recv error on block channel")?;
                let processed_block = process_block(rpc_client.as_ref(), slot, commitment_config).await;
                match processed_block {
                    Some(processed_block) => {
                        block_notification_sender.send(processed_block).context("Processed block should be sent")?;
                        // schedule to get finalized commitment
                        if commitment_config != CommitmentConfig::finalized() {
                            let retry_at = tokio::time::Instant::now()
                            .checked_add(Duration::from_secs(5))
                            .unwrap();
                            slot_retry_queue_sx.send(((slot, CommitmentConfig::finalized()), retry_at)).context("should be able to rescheduled for replay")?;
                        }
                    },
                    None => {
                        let retry_at = tokio::time::Instant::now()
                            .checked_add(Duration::from_millis(10))
                            .unwrap();
                        slot_retry_queue_sx.send(((slot, commitment_config), retry_at)).context("should be able to rescheduled for replay")?;
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
            while let Some(((slot, commitment_config), instant)) = slot_retry_queue_rx.recv().await {
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
                if block_schedule_queue_sx.send((slot, commitment_config)).await.is_err() {
                    bail!("could not schedule replay for a slot")
                }
            }
            unreachable!();
        });
        tasks.push(replay_task)
    }

    //slot poller
    let slot_poller = tokio::spawn(async move {
        log::info!("block listner started");
        let mut slot_notification = slot_notification;
        loop {
            let SlotNotification{
                estimated_processed_slot,
                ..
            } = slot_notification.recv().await.context("Should get slot notification")?;
            let last_slot = recent_slot.load(std::sync::atomic::Ordering::Relaxed);
            if last_slot < estimated_processed_slot {
                recent_slot.store(last_slot, std::sync::atomic::Ordering::Relaxed);
                for slot in last_slot+1..estimated_processed_slot+1 {
                    block_schedule_queue_sx.send((slot, CommitmentConfig::confirmed())).await.context("Should be able to schedule message")?;
                };
            }
        }
    });
    tasks.push(slot_poller);

    tasks
}