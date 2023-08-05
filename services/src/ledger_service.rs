use std::{marker::PhantomData, sync::Arc};

use bytes::Bytes;
use solana_lite_rpc_core::{
    jsonrpc_client::JsonRpcClient, ledger::Ledger, slot_clock::SlotClock, AnyhowJoinHandle,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::sync::mpsc;

use crate::{grpc_listener::GrpcListener, rpc_listener::RpcListener};

/// Rpc LedgerProvider Marker
pub struct RpcLedgerProvider;

/// Grpc LedgerProvider Marker
pub struct GrpcLedgerProvider;

/// Get's ledger data from various services
#[derive(Default)]
pub struct LedgerService<Provider> {
    ledger: Ledger,
    // Todo: add postgres stuff
    provider: PhantomData<Provider>,
}

impl<Provider> From<Ledger> for LedgerService<Provider> {
    fn from(value: Ledger) -> Self {
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
        let _grpc_listener: AnyhowJoinHandle = tokio::spawn(async move {
            //loop {
            let Err(_err) = GrpcListener::listen(addr, slot_tx, processed_tx, confirmed_tx, finalized_tx).await else {
                    anyhow::bail!("GrpcListner exited unexpectedly");
                };
            Ok(())
            // }
        });

        // process all the data
        let _processor = tokio::spawn(async move {});

        Ok(())
    }
}

impl LedgerService<RpcLedgerProvider> {
    pub async fn listen(self, addr: String) -> anyhow::Result<()> {
        let rpc_client = Arc::new(RpcClient::new(addr));
        let rpc_listener = RpcListener::new(rpc_client.clone());

        // slot unbounded channel
        let (slot_tx, slot_rx) = mpsc::unbounded_channel();

        // slot clock
        let slot_clock = SlotClock::default();
        // channels
        let (block_channel, _block_recv) = mpsc::unbounded_channel();

        // get processed slots
        let slot_lisner: AnyhowJoinHandle = tokio::spawn(async move {
            // Todo: poll for some errors else exit
            //loop {
            let Err(_err) = JsonRpcClient::poll_slots(&rpc_client, slot_tx.clone(), CommitmentConfig::processed()).await else {
                    anyhow::bail!("Rpc slot poll task unexpectedly");
                };
            Ok(())
            //}
        });

        let rpc_listener: AnyhowJoinHandle = tokio::spawn(rpc_listener.clone().listen(
            slot_clock,
            slot_rx,
            block_channel,
        ));

        // Todo: process all the data
        let processor = tokio::spawn(async move {});

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
