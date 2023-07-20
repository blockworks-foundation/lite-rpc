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

pub struct GrpcListener {
}

impl GrpcListener {
    pub async fn listen(addr: impl Into<Bytes>) -> anyhow::Result<()> {
        let mut client = GeyserGrpcClient::connect(addr, None::<&'static str>, None)?;

        let version = client.get_version().await?.version;

        if version != GRPC_VERSION {
            log::warn!("Expected version {:?}, got {:?}", GRPC_VERSION, version);
        }

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
}
