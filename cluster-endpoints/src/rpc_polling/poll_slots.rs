use std::{sync::Arc, time::Duration};

use anyhow::{bail, Context};
use debug_collections::tokio_wrapped::mpsc::channels_wrapped::send_timed;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::{structures::slot_notification::SlotNotification, AnyhowJoinHandle};
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};
use tokio::sync::broadcast::Sender;
const AVERAGE_SLOT_CHANGE_TIME: Duration = Duration::from_millis(400);

pub async fn poll_commitment_slots(
    rpc_client: Arc<RpcClient>,
    commitment_config: CommitmentConfig,
    slot_tx: tokio::sync::mpsc::Sender<Slot>,
) -> anyhow::Result<()> {
    let mut poll_frequency = tokio::time::interval(Duration::from_millis(50));
    let mut last_slot = 0;
    let mut errors = 0;
    loop {
        let slot = rpc_client.get_slot_with_commitment(commitment_config).await;
        match slot {
            Ok(slot) => {
                if slot > last_slot {
                    // send
                    send_timed(slot, &slot_tx).await.context("Error sending slot")?;
                    last_slot = slot;
                }
                errors = 0;
            }
            Err(e) => {
                errors += 1;
                if errors > 10 {
                    bail!("Exceeded error count to get slots from rpc {e:?}");
                }
            }
        }
        // wait for next poll i.e at least 50ms
        poll_frequency.tick().await;
    }
}

pub fn poll_slots(
    rpc_client: Arc<RpcClient>,
    commitment_config: CommitmentConfig,
    sender: Sender<SlotNotification>,
) -> anyhow::Result<Vec<AnyhowJoinHandle>> {
    // processed slot update task
    let (slot_update_sx, mut slot_update_rx) = tokio::sync::mpsc::channel(99);
    let task1 = tokio::spawn(poll_commitment_slots(
        rpc_client.clone(),
        commitment_config,
        slot_update_sx,
    ));
    let task2 = tokio::spawn(async move {
        let slot = rpc_client
            .get_slot_with_commitment(CommitmentConfig::confirmed())
            .await
            .context("Error getting slot")?;

        let mut current_slot = slot;
        let mut estimated_slot = slot;

        loop {
            match tokio::time::timeout(AVERAGE_SLOT_CHANGE_TIME, slot_update_rx.recv()).await {
                Ok(Some(slot)) => {
                    // slot is latest
                    if slot > current_slot {
                        current_slot = slot;
                        if current_slot > estimated_slot {
                            estimated_slot = slot;
                        }
                        sender
                            .send(SlotNotification {
                                processed_slot: current_slot,
                                estimated_processed_slot: estimated_slot,
                            })
                            .context("Cannot send slot notification")?;
                    }
                }
                Ok(None) => log::error!("got nothing from slot update notifier"),
                Err(err) => {
                    log::debug!("timeout on receive slot update: {err}");
                    // force update the slot
                    // estimated slot should not go ahead more than 32 slots
                    // this is because it may be a slot block
                    if estimated_slot < current_slot + 32 {
                        estimated_slot += 1;

                        sender
                            .send(SlotNotification {
                                processed_slot: current_slot,
                                estimated_processed_slot: estimated_slot,
                            })
                            .context("Cannot send slot notification")?;
                    }
                }
            }
        }
    });
    Ok(vec![task1, task2])
}
