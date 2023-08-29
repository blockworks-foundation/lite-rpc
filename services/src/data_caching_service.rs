use anyhow::bail;
use solana_lite_rpc_core::{AnyhowJoinHandle, structures::slot_notification::SlotNotification};
use solana_lite_rpc_core::block_information_store::BlockMeta;
use solana_lite_rpc_core::data_cache::DataCache;
use solana_lite_rpc_core::structures::processed_block::ProcessedBlock;
use solana_sdk::clock::MAX_RECENT_BLOCKHASHES;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_transaction_status::{TransactionConfirmationStatus, TransactionStatus};
use tokio::sync::broadcast::Receiver;

/// Get's ledger data from various services
#[derive(Default)]
pub struct DataCachingService {
    pub data_cache: DataCache,
}

impl DataCachingService {
    pub fn listen(
        self,
        block_notifier: Receiver<ProcessedBlock>,
        slot_notification: Receiver<SlotNotification>,
    ) -> Vec<AnyhowJoinHandle> {
        // clone the ledger to move into the processor task
        let data_cache = self.data_cache.clone();
        // process all the data into the ledger
        let block_cache_jh = tokio::spawn(async move {
            let mut block_notifier = block_notifier;
            loop {
                let ProcessedBlock {
                    txs,
                    leader_id: _,
                    blockhash,
                    block_height,
                    slot,
                    parent_slot: _,
                    block_time: _,
                    commitment_config,
                } = block_notifier.recv().await.expect("Should recv blocks");

                data_cache
                    .block_store
                    .add_block(
                        BlockMeta {
                            slot,
                            block_height,
                            last_valid_blockheight: block_height + MAX_RECENT_BLOCKHASHES as u64,
                            cleanup_slot: block_height + 1000,
                            //TODO: see why this was required
                            processed_local_time: None,
                            blockhash,
                        },
                        commitment_config,
                    )
                    .await;

                let confirmation_status = match commitment_config.commitment {
                    CommitmentLevel::Finalized => TransactionConfirmationStatus::Finalized,
                    _ => TransactionConfirmationStatus::Confirmed,
                };

                for tx in txs {
                    //
                    data_cache.txs.update_status(
                        &tx.signature,
                        TransactionStatus {
                            slot,
                            confirmations: None,
                            status: tx.status.clone(),
                            err: tx.err.clone(),
                            confirmation_status: Some(confirmation_status.clone()),
                        },
                    );
                    // notify
                    data_cache
                        .tx_subs
                        .notify_tx(slot, &tx, commitment_config)
                        .await;
                }
            }
        });

        let data_cache = self.data_cache;
        let slot_cache_jh = tokio::spawn(async move {
            let mut slot_notification = slot_notification;
            loop {
                match slot_notification.recv().await {
                    Ok(slot_notification) => {
                        data_cache.slot_cache.update(slot_notification);
                    },
                    Err(e) => {
                        bail!("Error in slot notification {e:?}");
                    }
                }
            }
        });
        vec![slot_cache_jh, block_cache_jh]
    }
}
