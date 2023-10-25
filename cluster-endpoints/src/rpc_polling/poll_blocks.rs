use anyhow::Context;
use prometheus::{core::GenericGauge, opts, register_int_gauge};
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

lazy_static::lazy_static! {
    static ref NB_BLOCK_FETCHING_TASKS: GenericGauge<prometheus::core::AtomicI64> = register_int_gauge!(opts!("literpc_num_blockfetching_tasks", "Transactions in store")).unwrap();
}

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

pub async fn check_finalized(rpc_client: &RpcClient, slot: Slot, blockhash: &String) -> bool {
    let block = rpc_client
        .get_block_with_config(
            slot,
            RpcBlockConfig {
                transaction_details: None,
                commitment: Some(CommitmentConfig::finalized()),
                max_supported_transaction_version: Some(0),
                encoding: Some(UiTransactionEncoding::Base64),
                rewards: None,
            },
        )
        .await;
    match block {
        Ok(block) => {
            if block.blockhash != *blockhash {
                log::error!(
                    "blockhash mismatch confirmed : {} finalized : {} for slot: {}",
                    blockhash,
                    block.blockhash,
                    slot
                );
            }
            true
        }
        Err(_) => false,
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
                    NB_BLOCK_FETCHING_TASKS.inc();
                    let rpc_client = rpc_client.clone();
                    let block_notification_sender = block_notification_sender.clone();
                    let current_slot = current_slot.clone();
                    tokio::spawn(async move {
                        let mut confirmed_block = None;
                        while current_slot
                            .load(std::sync::atomic::Ordering::Relaxed)
                            .saturating_sub(slot)
                            < 64
                        {
                            if let Some(produced_block) = process_block(
                                rpc_client.as_ref(),
                                slot,
                                CommitmentConfig::confirmed(),
                            )
                            .await
                            {
                                let _ = block_notification_sender.send(produced_block.clone());
                                confirmed_block = Some(produced_block);
                                tokio::time::sleep(Duration::from_secs(2)).await;
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        while current_slot
                            .load(std::sync::atomic::Ordering::Relaxed)
                            .saturating_sub(slot)
                            < 256
                        {
                            if let Some(confirmed_block) = &confirmed_block {
                                if check_finalized(
                                    rpc_client.as_ref(),
                                    slot,
                                    &confirmed_block.blockhash,
                                )
                                .await
                                {
                                    let mut finalized_block = confirmed_block.clone();
                                    finalized_block.commitment_config =
                                        CommitmentConfig::finalized();
                                    let _ = block_notification_sender.send(finalized_block);
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            } else {
                                break;
                            }
                        }
                        NB_BLOCK_FETCHING_TASKS.dec();
                        drop(premit)
                    });
                }
            }
        }
    });

    vec![task_spawner]
}
