use std::{marker::PhantomData, sync::Arc};

use bytes::Bytes;
use solana_lite_rpc_core::{jsonrpc_client::JsonRpcClient, ledger::Ledger, AnyhowJoinHandle};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::sync::{broadcast, mpsc};

use crate::{grpc_listener::GrpcListener, rpc_listener::RpcListener, slot_clock::SlotClock};

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

impl LedgerService<GrpcLedgerProvider> {
    pub async fn listen(self, addr: impl Into<Bytes> + Sync + Send) -> anyhow::Result<()> {
        // slot unbounded channel
        let (slot_tx, slot_rx) = mpsc::unbounded_channel();
        // tx ubouded channel
        let (processed_tx, processed_rx) = mpsc::unbounded_channel();
        let (confirmed_tx, confirmed_rx) = mpsc::unbounded_channel();
        let (finalized_tx, finalized_rx) = mpsc::unbounded_channel();

        // do some error counting
        let grpc_listener: AnyhowJoinHandle = tokio::spawn(async move {
            loop {
                let Err(err) = GrpcListener::listen(addr, slot_tx, processed_tx, confirmed_tx, finalized_tx).await else {
                    anyhow::bail!("GrpcListner exited unexpectedly");
                };
            }
        });

        // process all the data
        let processor = tokio::spawn(async move {});

        Ok(())
    }
}

impl LedgerService<RpcLedgerProvider> {
    pub async fn listen(self, addr: String) -> anyhow::Result<()> {
        let rpc_client = Arc::new(RpcClient::new(addr));
        let rpc_listener = RpcListener::new(rpc_client.clone());

        // slot broadcast channel
        let (slot_tx, slot_rx) = broadcast::channel(1);
        // slot clock
        let slot_clock = SlotClock::default();
        // channels
        let (processed_block_tx, processed_block_rx) = mpsc::unbounded_channel();
        let (confirmed_block_tx, confirmed_block_rx) = mpsc::unbounded_channel();
        let (finalized_block_tx, finalized_block_rx) = mpsc::unbounded_channel();

        // get processed slots
        let slot_lisner: AnyhowJoinHandle = tokio::spawn(async move {
            // Todo: poll for some errors else exit
            loop {
                let Err(err) = JsonRpcClient::poll_slots(&rpc_client, slot_tx, CommitmentConfig::processed()).await else {
                    anyhow::bail!("Rpc slot poll task unexpectedly");
                };
            }
        });

        let rpc_listener: AnyhowJoinHandle = tokio::spawn(async move {
            // get data from rpc
            // processed
            let processed_listner = rpc_listener.clone().listen(
                slot_rx,
                processed_block_tx,
                CommitmentConfig::processed(),
            );
            // confirmed
            let confirmed_listner = rpc_listener.clone().listen(
                slot_rx,
                confirmed_block_tx,
                CommitmentConfig::confirmed(),
            );
            // finalized
            let finalized_listner = rpc_listener.clone().listen(
                slot_rx,
                finalized_block_tx,
                CommitmentConfig::finalized(),
            );

            tokio::select! {
                processed_res = processed_listner => {
                    anyhow::bail!("Processed stream closed unexpectedly {processed_res:?}");
                }
                confirmed_res = confirmed_listner => {
                    anyhow::bail!("Confirmed stream closed unexpectedly {confirmed_res:?}");
                }
                finalized_res = finalized_listner => {
                    anyhow::bail!("Finalized stream closed unexpectedly {finalized_res:?}");
                }
            }
        });

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
