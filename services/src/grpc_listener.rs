use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};

use anyhow::bail;
use bytes::Bytes;
use futures_util::StreamExt;

use solana_lite_rpc_core::AtomicSlot;
use solana_sdk::slot_history::Slot;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    Semaphore,
};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::{
    prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeUpdateSlot,
    },
    tonic::service::Interceptor,
};

const GRPC_URL: &str = "http://127.0.0.0:10000";
const GRPC_VERSION: &str = "1.16.1";

pub struct GrpcListener;

impl GrpcListener {
    pub async fn listen(addr: impl Into<Bytes>) -> anyhow::Result<()> {
        let mut client = GeyserGrpcClient::connect(addr, None::<&'static str>, None)?;

        let version = client.get_version().await?.version;

        if version != GRPC_VERSION {
            log::warn!("Expected version {:?}, got {:?}", GRPC_VERSION, version);
        }

        let (slot_tx, slot_rx) = mpsc::unbounded_channel(); // don't introduce back pressure
        let (block_tx, block_rx) = mpsc::unbounded_channel(); // don't introduce back pressure

        let slot_producer = tokio::spawn(Self::subscribe(client, slot_tx, block_tx));
        let block_indexer = tokio::spawn(Self::index_blocks(slot_rx));

        tokio::select! {
            _ = slot_producer => {
                bail!("Slot stream closed unexpectedly");
            }
            _ = block_indexer => {
                bail!("Block indexer closed unexpectedly");
            }
        }
    }

    pub async fn index_blocks(mut rx: UnboundedReceiver<Slot>) -> anyhow::Result<()> {
        let block_worker_semaphore = Arc::new(Semaphore::new(MAX_BLOCK_INDEXERS));

        while let Some(slot) = rx.recv().await {
            println!(
                "Block indexers running {:?}/MAX_BLOCK_INDEXERS",
                MAX_BLOCK_INDEXERS - block_worker_semaphore.available_permits()
            );

            let permit = block_worker_semaphore.clone().acquire_owned().await?;

            tokio::spawn(async move {
                index_block_for_slot(slot).await.unwrap();

                drop(permit);
            });
        }

        bail!("Slot stream closed unexpectedly");
    }

    pub async fn index_block_for_slot(slot: Slot) -> anyhow::Result<()> {
        println!("Processing slot: {:?}", slot);
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        Ok(())
    }

    pub async fn subscribe<F: Interceptor>(
        mut client: GeyserGrpcClient<F>,
        atomic_slot: Option<AtomicSlot>,
        tx_sender: UnboundedSender<Transactiodwn>,
        commitment: CommitmentLevel,
    ) -> anyhow::Result<()> {
        // check if atomic_slot is None
        // subscribe to slot only if atomic_slot is Some
        let mut slots = HashMap::new();
        if atomic_slot.is_some() {
            slots.insert("client".to_string(), SubscribeRequestFilterSlots {});
        }

        // subscribe to transactions
        let mut txs = HashMap::new();
        txs.insert(
            "client".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: Some(true), // get failed transactions as well
                signature: None,
                account_include: Default::default(),
                account_exclude: Default::default(),
                account_required: Default::default(),
            },
        );

        let mut stream = client
            .subscribe_once(
                slots,
                Default::default(),
                txs,
                Default::default(),
                Default::default(),
                Some(commitment),
                Default::default(),
            )
            .await?;

        while let Some(message) = stream.next().await {
            let message = message?;

            let Some(update) = message.update_oneof else {
                continue;
            };

            match update {
                UpdateOneof::Slot(SubscribeUpdateSlot { slot, .. }) => {
                    let Some(atomic_slot) = atomic_slot.as_ref() else {
                        bail!("Unexpected slot notification");
                    };

                    atomic_slot.store(slot, Ordering::Relaxed);
                }
                UpdateOneof::Transaction(tx) => {
                    tx_sender.send(tx)?;
                }
                UpdateOneof::Ping(_) => {
                    log::trace!("GRPC Ping {commitment:?}");
                }
                k => {
                    bail!("Unexpected update: {k:?}");
                }
            };
        }

        bail!("Stream closed unexpectedly")
    }
}
