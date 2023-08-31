use std::time::Duration;

use anyhow::{bail, Context};
use solana_lite_rpc_core::block_information_store::BlockInformation;
use solana_lite_rpc_core::data_cache::DataCache;
use solana_lite_rpc_core::streams::{
    BlockStream, ClusterInfoStream, SlotStream, VoteAccountStream,
};
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_transaction_status::{TransactionConfirmationStatus, TransactionStatus};

pub struct DataCachingService {
    pub data_cache: DataCache,
    pub clean_duration: Duration,
}

impl DataCachingService {
    pub fn listen(
        self,
        block_notifier: BlockStream,
        slot_notification: SlotStream,
        cluster_info_notification: ClusterInfoStream,
        va_notification: VoteAccountStream,
    ) -> Vec<AnyhowJoinHandle> {
        // clone the ledger to move into the processor task
        let data_cache = self.data_cache.clone();
        // process all the data into the ledger
        let block_cache_jh = tokio::spawn(async move {
            let mut block_notifier = block_notifier;
            loop {
                let block = block_notifier.recv().await.expect("Should recv blocks");
                data_cache
                    .block_store
                    .add_block(
                        BlockInformation::from_block(&block),
                        block.commitment_config,
                    )
                    .await;

                let confirmation_status = match block.commitment_config.commitment {
                    CommitmentLevel::Finalized => TransactionConfirmationStatus::Finalized,
                    _ => TransactionConfirmationStatus::Confirmed,
                };

                for tx in block.txs {
                    //
                    data_cache.txs.update_status(
                        &tx.signature,
                        TransactionStatus {
                            slot: block.slot,
                            confirmations: None,
                            status: tx.err.clone().map_or(Ok(()), Err),
                            err: tx.err.clone(),
                            confirmation_status: Some(confirmation_status.clone()),
                        },
                    );
                    // notify
                    data_cache
                        .tx_subs
                        .notify(block.slot, &tx, block.commitment_config)
                        .await;
                }
            }
        });

        let data_cache = self.data_cache.clone();
        let slot_cache_jh = tokio::spawn(async move {
            let mut slot_notification = slot_notification;
            loop {
                match slot_notification.recv().await {
                    Ok(slot_notification) => {
                        data_cache.slot_cache.update(slot_notification);
                    }
                    Err(e) => {
                        bail!("Error in slot notification {e:?}");
                    }
                }
            }
        });

        let data_cache = self.data_cache.clone();
        let cluster_info_jh = tokio::spawn(async move {
            let mut cluster_info_notification = cluster_info_notification;
            loop {
                data_cache
                    .cluster_info
                    .load_cluster_info(&mut cluster_info_notification)
                    .await?;
            }
        });

        let data_cache: DataCache = self.data_cache.clone();
        let identity_stakes_jh = tokio::spawn(async move {
            let mut va_notification = va_notification;
            loop {
                let vote_accounts = va_notification
                    .recv()
                    .await
                    .context("Could not get vote accounts")?;
                data_cache
                    .identity_stakes
                    .update_stakes_for_identity(vote_accounts)
                    .await;
            }
        });

        let data_cache: DataCache = self.data_cache;
        let clean_ttl = self.clean_duration;
        let cleaning_service = tokio::spawn(async move {
            loop {
                // clean frequency 1min
                tokio::time::sleep(Duration::from_secs(60)).await;
                data_cache.clean(clean_ttl).await;
            }
        });
        vec![
            slot_cache_jh,
            block_cache_jh,
            cluster_info_jh,
            identity_stakes_jh,
            cleaning_service,
        ]
    }
}
