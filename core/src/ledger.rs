use std::marker::PhantomData;

use bytes::Bytes;
use solana_sdk::slot_history::Slot;
use tokio::sync::mpsc::UnboundedSender;
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction;

use crate::block_store::Block;
use crate::AnyhowJoinHandle;
use crate::{block_store::BlockStore, slot_clock::SlotClock, tx_store::TxStore};

/// Rpc LedgerProvider Marker
pub struct RpcLedgerProvider;

/// Grpc LedgerProvider Marker
pub struct GrpcLedgerProvider;

/// The central data store for all data from the cluster.
#[derive(Default)]
pub struct Ledger<Provider> {
    blocks: BlockStore,
    clock: SlotClock,
    txs: TxStore,
    provider: PhantomData<Provider>,
}

impl Ledger<GrpcLedgerProvider> {
    pub fn listen(self, addr: impl Into<Bytes>) -> anyhow::Result<()> {
        // slot unbounded channel
        let (slot_tx, slot_rx) = mpsc::unbounded_channel();
        // tx ubouded channel
        let (processed_tx, processed_rx) = mpsc::unbounded_channel();
        let (confirmed_tx, confirmed_rx) = mpsc::unbounded_channel();
        let (finalized_tx, finalized_rx) = mpsc::unbounded_channel();

        /// do some error counting
        let grpc_listener = tokio::spawn(async move {
            loop {
                let Err(err) =  GrpcListener::listen(addr, slot_tx, processed_tx, confirmed_tx, finalized_tx) {
                    bail!("GrpcListner exited unexpectedly");
                };
            }
        });

        /// process all the data 
        let processor = tokio::spawn(async move {
            loop {
                let Err(err) = Ledger::process(slot_rx, processed_rx, confirmed_rx, finalized_rx).await {
                    bail!("Ledger processor exited unexpectedly");
                };
            }
        });

        Ok(())
    }
}

impl Ledger<RpcLedgerProvider> {
    pub fn listen(self, addr: &str) -> anyhow::Result<()> {
    }
}
