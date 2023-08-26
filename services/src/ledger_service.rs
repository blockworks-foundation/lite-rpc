use std::{marker::PhantomData, sync::Arc};

use anyhow::Context;
use bytes::Bytes;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_lite_rpc_core::block_information_store::BlockMeta;
use solana_lite_rpc_core::data_cache::DataCache;
use solana_lite_rpc_core::grpc_client::GrpcClient;
use solana_lite_rpc_core::jsonrpc_client::JsonRpcClient;
use solana_lite_rpc_core::processed_block::ProcessedBlock;
use solana_lite_rpc_core::slot_clock::SlotClock;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
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
    data_cache: DataCache,
    // Todo: add postgres stuff
    provider: PhantomData<Provider>,
}

impl<Provider> From<DataCache> for LedgerService<Provider> {
    fn from(value: DataCache) -> Self {
        Self {
            data_cache: value,
            provider: PhantomData,
        }
    }
}

impl<T> LedgerService<T> {
    pub async fn process_block(
        self,
        mut block_recv: UnboundedReceiver<ProcessedBlock>,
    ) -> anyhow::Result<()> {
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
            self.data_cache
                .block_store
                .add_block(
                    BlockMeta::new(slot, block_height, blockhash),
                    commitment_config,
                )
                .await;

            let confirmation_status = match commitment_config.commitment {
                CommitmentLevel::Finalized => TransactionConfirmationStatus::Finalized,
                _ => TransactionConfirmationStatus::Confirmed,
            };

            for tx in txs {
                //
                self.data_cache.txs.update_status(
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
                self.data_cache
                    .tx_subs
                    .notify_tx(slot, &tx, commitment_config)
                    .await;
            }
        }

        anyhow::bail!("Block stream closed unexpectedly");
    }
}

impl LedgerService<GrpcLedgerProvider> {
    pub async fn listen(
        self,
        addr: impl Into<Bytes> + Sync + Send + Clone + 'static,
    ) -> anyhow::Result<()> {
        // slot clock
        let slot_clock = SlotClock::default();
        // slot unbounded channel
        let (slot_channel, mut slot_rx) = mpsc::unbounded_channel();
        let (block_tx, mut block_rx) = mpsc::unbounded_channel();

        // slot producer
        let slot_sub: AnyhowJoinHandle = tokio::spawn(GrpcClient::subscribe(
            GrpcClient::create_client(addr.clone()).await?,
            Some(slot_channel),
            None,
            None,
            CommitmentConfig::processed(),
        ));

        // confirmed block producer
        let confirmed_block_producer: AnyhowJoinHandle = tokio::spawn(GrpcClient::subscribe(
            GrpcClient::create_client(addr.clone()).await?,
            None,
            None,
            Some(block_tx),
            CommitmentConfig::confirmed(),
        ));

        // finalized block producer
        let finalized_block_producer: AnyhowJoinHandle = tokio::spawn(GrpcClient::subscribe(
            GrpcClient::create_client(addr.clone()).await?,
            None,
            None,
            Some(block_tx),
            CommitmentConfig::finalized(),
        ));

        // slot processor
        let slot_processor: AnyhowJoinHandle = tokio::spawn(async move {
            loop {
                slot_clock.set_slot(&mut slot_rx).await;
            }
        });

        // block processor
        let processor = tokio::spawn(self.process_block(block_rx));

        tokio::select! {
            slot_res = slot_sub => {
                anyhow::bail!("Slot stream closed unexpectedly {slot_res:?}");
            }
            slot_processor_res = slot_processor => {
                anyhow::bail!("Slot processor stream closed unexpectedly {slot_processor_res:?}");
            }
            confirmed_block_res = confirmed_block_producer => {
                anyhow::bail!("Confirmed block stream closed unexpectedly {confirmed_block_res:?}");
            }
            finalized_block_res = finalized_block_producer => {
                anyhow::bail!("Finalized block stream closed unexpectedly {finalized_block_res:?}");
            }
            processor_res = processor => {
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
        let (slot_channel, slot_recv) = mpsc::unbounded_channel();
        let (block_channel, block_recv) = mpsc::unbounded_channel();

        // get processed slots
        let slot_lisner: AnyhowJoinHandle = tokio::spawn(async move {
            JsonRpcClient::poll_slots(&rpc_client, slot_channel, CommitmentConfig::confirmed())
                .await
                .context("Rpc slot poll task unexpectedly")
        });

        // listen to blocks for slots
        let rpc_listener: AnyhowJoinHandle =
            tokio::spawn(rpc_listener.listen(slot_clock, slot_recv, block_channel));

        // process all the data into the ledger
        let processor = tokio::spawn(self.process_block(block_recv));

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
