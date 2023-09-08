use anyhow::Context;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::{
    structures::{
        processed_block::{ProcessedBlock, TransactionInfo},
        slot_notification::SlotNotification,
    },
    AnyhowJoinHandle,
};
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::{
    borsh0_10::try_from_slice_unchecked,
    commitment_config::CommitmentConfig,
    compute_budget::{self, ComputeBudgetInstruction},
    slot_history::Slot,
};
use solana_transaction_status::{
    option_serializer::OptionSerializer, RewardType, TransactionDetails, UiTransactionEncoding,
    UiTransactionStatusMeta,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::broadcast::{Receiver, Sender};

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

    let txs = txs
        .into_iter()
        .filter_map(|tx| {
            let Some(UiTransactionStatusMeta {
                err,
                compute_units_consumed,
                ..
            }) = tx.meta
            else {
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
                cu_requested,
                prioritization_fees,
                cu_consumed,
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

pub fn poll_block(
    rpc_client: Arc<RpcClient>,
    block_notification_sender: Sender<ProcessedBlock>,
    slot_notification: Receiver<SlotNotification>,
) -> Vec<AnyhowJoinHandle> {
    let task_spawner: AnyhowJoinHandle = tokio::spawn(async move {
        let counting_semaphore = Arc::new(tokio::sync::Semaphore::new(1024));
        let mut slot_notification = slot_notification;
        let mut last_processed_slot = 0;
        loop {
            let SlotNotification { processed_slot, .. } = slot_notification
                .recv()
                .await
                .context("Slot notification channel close")?;
            if processed_slot > last_processed_slot {
                last_processed_slot = processed_slot;
                let premit = counting_semaphore.clone().acquire_owned().await?;
                let rpc_client = rpc_client.clone();
                let block_notification_sender = block_notification_sender.clone();
                tokio::spawn(async move {
                    // try 500 times because slot gets
                    for _ in 0..1024 {
                        if let Some(processed_block) = process_block(
                            rpc_client.as_ref(),
                            processed_slot,
                            CommitmentConfig::confirmed(),
                        )
                        .await
                        {
                            let _ = block_notification_sender.send(processed_block);
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }

                    for _ in 0..1024 {
                        if let Some(processed_block) = process_block(
                            rpc_client.as_ref(),
                            processed_slot,
                            CommitmentConfig::finalized(),
                        )
                        .await
                        {
                            let _ = block_notification_sender.send(processed_block);
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    drop(premit)
                });
            }
        }
    });

    vec![task_spawner]
}
