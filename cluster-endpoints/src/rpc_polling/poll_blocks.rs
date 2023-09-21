use anyhow::Context;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::{
    structures::{produced_block::ProducedBlock, slot_notification::SlotNotification},
    AnyhowJoinHandle,
};
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};
use tokio::sync::broadcast::{Receiver, Sender};

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

    match block {
        Ok(block) => Some(ProducedBlock::from_ui_block(block, slot, commitment_config)),
        Err(_) => None,
    }
}

pub fn poll_block(
    rpc_client: Arc<RpcClient>,
    block_notification_sender: Sender<ProducedBlock>,
    slot_notification: Receiver<SlotNotification>,
) -> Vec<AnyhowJoinHandle> {
    let task_spawner: AnyhowJoinHandle = tokio::spawn(async move {
        let counting_semaphore = Arc::new(tokio::sync::Semaphore::new(1024));
        let mut slot_notification = slot_notification;
        let current_slot = Arc::new(AtomicU64::new(0));
        loop {
            let SlotNotification { processed_slot, .. } = slot_notification
                .recv()
                .await
                .context("Slot notification channel close")?;
            let last_processed_slot = current_slot.load(std::sync::atomic::Ordering::Relaxed);
            let last_processed_slot = if last_processed_slot == 0 {
                processed_slot.saturating_sub(1)
            } else {
                last_processed_slot
            };
            if processed_slot > last_processed_slot {
                current_slot.store(processed_slot, std::sync::atomic::Ordering::Relaxed);

                for slot in last_processed_slot + 1..processed_slot + 1 {
                    let premit = counting_semaphore.clone().acquire_owned().await?;
                    let rpc_client = rpc_client.clone();
                    let block_notification_sender = block_notification_sender.clone();
                    let current_slot = current_slot.clone();
                    tokio::spawn(async move {
                        let mut confirmed_slot_fetch = false;
                        while current_slot
                            .load(std::sync::atomic::Ordering::Relaxed)
                            .saturating_sub(slot)
                            < 32
                        {
                            if let Some(processed_block) = process_block(
                                rpc_client.as_ref(),
                                slot,
                                CommitmentConfig::confirmed(),
                            )
                            .await
                            {
                                let _ = block_notification_sender.send(processed_block);
                                confirmed_slot_fetch = true;
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }

                        while confirmed_slot_fetch
                            && current_slot
                                .load(std::sync::atomic::Ordering::Relaxed)
                                .saturating_sub(slot)
                                < 128
                        {
                            if let Some(processed_block) = process_block(
                                rpc_client.as_ref(),
                                slot,
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
        }
    });

    vec![task_spawner]
}
