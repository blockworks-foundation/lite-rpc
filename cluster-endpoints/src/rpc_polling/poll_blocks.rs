use anyhow::{bail, Context};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::{
    structures::{
        produced_block::ProducedBlock,
        slot_notification::{AtomicSlot, SlotNotification},
    },
    AnyhowJoinHandle,
};
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    slot_history::Slot,
};
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};
use std::{sync::Arc, time::Duration};
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
    block
        .ok()
        .map(|block| ProducedBlock::from_ui_block(block, slot, commitment_config))
}

pub fn poll_block(
    rpc_client: Arc<RpcClient>,
    block_notification_sender: Sender<ProducedBlock>,
    slot_notification: Receiver<SlotNotification>,
) -> Vec<AnyhowJoinHandle> {
    let mut tasks: Vec<AnyhowJoinHandle> = vec![];

    let recent_slot = AtomicSlot::default();
    let (slot_retry_queue_sx, mut slot_retry_queue_rx) = tokio::sync::mpsc::unbounded_channel();
    let (block_schedule_queue_sx, block_schedule_queue_rx) =
        async_channel::unbounded::<(Slot, CommitmentConfig)>();

    for _i in 0..16 {
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
