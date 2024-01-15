use std::time::Duration;

use anyhow::{bail, Context};
use prometheus::core::GenericGauge;
use prometheus::{opts, register_int_counter, register_int_gauge, IntCounter};
use solana_lite_rpc_core::stores::{
    block_information_store::BlockInformation, data_cache::DataCache,
};
use solana_lite_rpc_core::types::{BlockStream, ClusterInfoStream, SlotStream, VoteAccountStream};
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_sdk::clock::MAX_RECENT_BLOCKHASHES;
use solana_sdk::commitment_config::CommitmentLevel;
use solana_transaction_status::{TransactionConfirmationStatus, TransactionStatus};

lazy_static::lazy_static! {
    static ref NB_CLUSTER_NODES: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_nb_cluster_nodes", "Number of cluster nodes in saved")).unwrap();

    static ref CURRENT_SLOT: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_current_slot", "Current slot seen by last rpc")).unwrap();

    static ref ESTIMATED_SLOT: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_estimated_slot", "Estimated slot seen by last rpc")).unwrap();

    static ref TXS_CONFIRMED: IntCounter =
    register_int_counter!(opts!("literpc_txs_confirmed", "Number of Transactions Confirmed")).unwrap();

    static ref TXS_FINALIZED: IntCounter =
    register_int_counter!(opts!("literpc_txs_finalized", "Number of Transactions Finalized")).unwrap();

    static ref TXS_PROCESSED: IntCounter =
    register_int_counter!(opts!("literpc_txs_processed", "Number of Transactions Processed")).unwrap();
}

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
                    .block_information_store
                    .add_block(BlockInformation::from_block(&block))
                    .await;

                let confirmation_status = match block.commitment_config.commitment {
                    CommitmentLevel::Finalized => TransactionConfirmationStatus::Finalized,
                    CommitmentLevel::Confirmed => TransactionConfirmationStatus::Confirmed,
                    _ => TransactionConfirmationStatus::Processed,
                };

                for tx in block.transactions {
                    let block_info = data_cache
                        .block_information_store
                        .get_block_info(&tx.recent_blockhash);
                    let last_valid_blockheight = if let Some(block_info) = block_info {
                        block_info.last_valid_blockheight
                    } else {
                        block.block_height + MAX_RECENT_BLOCKHASHES as u64
                    };

                    if data_cache.txs.update_status(
                        &tx.signature,
                        TransactionStatus {
                            slot: block.slot,
                            confirmations: None,
                            status: tx.err.clone().map_or(Ok(()), Err),
                            err: tx.err.clone(),
                            confirmation_status: Some(confirmation_status.clone()),
                        },
                        last_valid_blockheight,
                    ) {
                        // transaction updated
                        match confirmation_status {
                            TransactionConfirmationStatus::Finalized => {
                                TXS_FINALIZED.inc();
                            }
                            TransactionConfirmationStatus::Confirmed => {
                                TXS_CONFIRMED.inc();
                            }
                            TransactionConfirmationStatus::Processed => {
                                TXS_PROCESSED.inc();
                            }
                        }
                    }
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
                        CURRENT_SLOT.set(slot_notification.processed_slot as i64);
                        ESTIMATED_SLOT.set(slot_notification.estimated_processed_slot as i64);
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
                NB_CLUSTER_NODES.set(data_cache.cluster_info.cluster_nodes.len() as i64);
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
