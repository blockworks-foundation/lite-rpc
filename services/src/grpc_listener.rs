use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};

use anyhow::bail;
use bytes::Bytes;
use futures_util::StreamExt;

use solana_lite_rpc_core::{grpc_client::GrpcClient, ledger::Ledger, AnyhowJoinHandle, AtomicSlot};
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

pub struct GrpcListener;

impl GrpcListener {
    pub async fn listen(
        addr: impl Into<Bytes>,
        slots_sender: UnboundedSender<Slot>,
        processed_tx: UnboundedSender<SubscribeUpdateTransaction>,
        confirmed_tx: UnboundedSender<SubscribeUpdateTransaction>,
        finalized_tx: UnboundedSender<SubscribeUpdateTransaction>,
    ) -> anyhow::Result<()> {
        let processed_future = GrpcClient::subscribe(
            GrpcClient::create_client(addr),
            Some(slot),
            Some(processed_tx),
            Some(CommitmentLevel::Processed),
        );

        let confirmed_future = GrpcClient::subscribe(
            GrpcClient::create_client(addr),
            None,
            Some(processed_tx),
            Some(CommitmentLevel::Confirmed),
        );

        let finalized_future = GrpcClient::subscribe(
            GrpcClient::create_client(addr),
            None,
            Some(processed_tx),
            Some(CommitmentLevel::Finalized),
        );

        tokio::select! {
            res = processed_future => {
                bail!("Processed stream closed unexpectedly {res}");
            }
            res = confirmed_future => {
                bail!("Confirmed stream closed unexpectedly {res}");
            }
            res = finalized_future => {
                bail!("Finalized stream closed unexpectedly {res}");
            }
        }
    }
}
