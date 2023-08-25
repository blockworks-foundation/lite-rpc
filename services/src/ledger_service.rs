use std::{marker::PhantomData, sync::Arc};

use anyhow::Context;
use bytes::Bytes;
use solana_lite_rpc_core::block_information_store::BlockMeta;
use solana_lite_rpc_core::jsonrpc_client::ProcessedBlock;
use solana_lite_rpc_core::{
    jsonrpc_client::JsonRpcClient, data_cache::DataCache, slot_clock::SlotClock, AnyhowJoinHandle,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::clock::MAX_RECENT_BLOCKHASHES;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};

use solana_transaction_status::{TransactionConfirmationStatus, TransactionStatus};
use tokio::sync::mpsc;

use crate::{grpc_listener::GrpcListener, rpc_listener::RpcListener};

/// Rpc LedgerProvider Marker
pub struct RpcLedgerProvider;

/// Grpc LedgerProvider Marker
pub struct GrpcLedgerProvider;

/// Get's ledger data from various services
#[derive(Default)]
pub struct LedgerService<Provider> {
    ledger: DataCache,
    // Todo: add postgres stuff
    provider: PhantomData<Provider>,
}

impl<Provider> From<DataCache> for LedgerService<Provider> {
    fn from(value: DataCache) -> Self {
        Self {
            ledger: value,
            provider: PhantomData,
        }
    }
}

impl LedgerService<GrpcLedgerProvider> {
    pub async fn listen(
        self,
        addr: impl Into<Bytes> + Sync + Send + Clone + 'static,
    ) -> anyhow::Result<()> {
        // slot unbounded channel
        let (slot_tx, _slot_rx) = mpsc::unbounded_channel();
        // tx ubouded channel
        let (processed_tx, _processed_rx) = mpsc::unbounded_channel();
        let (confirmed_tx, _confirmed_rx) = mpsc::unbounded_channel();
        let (finalized_tx, _finalized_rx) = mpsc::unbounded_channel();

        // do some error counting
        let grpc_listener: AnyhowJoinHandle = tokio::spawn(async move {
            let Err(_err) = GrpcListener::listen(addr, slot_tx, processed_tx, confirmed_tx, finalized_tx).await else {
                    anyhow::bail!("GrpcListner exited unexpectedly");
                };
            Ok(())
            // }
        });

        // process all the data
        let _processor = tokio::spawn(async move {
            futures::future::pending::<()>().await;
        });

        tokio::select! {
            grpc_res = grpc_listener => {
                anyhow::bail!("GrpcListner exited unexpectedly {grpc_res:?}");
            }
            processor_res = _processor => {
                anyhow::bail!("Processor stream closed unexpectedly {processor_res:?}");
            }
        }
    }
}

impl LedgerService<RpcLedgerProvider> {
    pub async fn listen(self, addr: String) -> anyhow::Result<()> {
        let rpc_client = Arc::new(RpcClient::new(addr));
        let rpc_listener = RpcListener::new(rpc_client.clone());

        // slot clock
        let slot_clock = SlotClock::default();
        // slot and block channels
        let (slot_tx, slot_rx) = mpsc::unbounded_channel();
        let (block_channel, mut block_recv) = mpsc::unbounded_channel();

        // get processed slots
        let slot_lisner: AnyhowJoinHandle = tokio::spawn(async move {
            JsonRpcClient::poll_slots(&rpc_client, slot_tx, CommitmentConfig::confirmed())
                .await
                .context("Rpc slot poll task unexpectedly")
        });

        // listen to blocks for slots
        let rpc_listener: AnyhowJoinHandle =
            tokio::spawn(rpc_listener.listen(slot_clock, slot_rx, block_channel));

        // clone the ledger to move into the processor task
        let ledger = self.ledger.clone();
        // process all the data into the ledger
        let processor = tokio::spawn(async move {
            while let Some(ProcessedBlock {
                txs,
                leader_id: _,
                blockhash,
                block_height,
                slot,
                parent_slot: _,
                block_time: _,
                commitment_config,
            }) = block_recv.recv().await
            {
                ledger
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
                    ledger.txs.update_status(
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
                    ledger.tx_subs.notify_tx(slot, &tx, commitment_config).await;
                }
            }
        });

        tokio::select! {
            slot_res = slot_lisner => {
                anyhow::bail!("Slot stream closed unexpectedly {slot_res:?}");
            }
            rpc_res = rpc_listener => {
                anyhow::bail!("Rpc stream closed unexpectedly {rpc_res:?}");
            }
            processor_res = processor => {
                anyhow::bail!("Processor stream closed unexpectedly {processor_res:?}");
            }
        }
    }
}
