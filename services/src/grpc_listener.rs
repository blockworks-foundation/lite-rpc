use anyhow::bail;
use bytes::Bytes;

use solana_lite_rpc_core::{grpc_client::GrpcClient};
use solana_sdk::slot_history::Slot;
use tokio::sync::mpsc::{UnboundedSender};
use yellowstone_grpc_proto::{
    prelude::{
        CommitmentLevel, SubscribeUpdateTransaction,
    },
};

pub struct GrpcListener;

impl GrpcListener {
    pub async fn listen(
        addr: impl Into<Bytes> + Clone,
        slots_sender: UnboundedSender<Slot>,
        processed_tx: UnboundedSender<SubscribeUpdateTransaction>,
        confirmed_tx: UnboundedSender<SubscribeUpdateTransaction>,
        finalized_tx: UnboundedSender<SubscribeUpdateTransaction>,
    ) -> anyhow::Result<()> {
        let processed_future = GrpcClient::subscribe(
            GrpcClient::create_client(addr.clone()).await?,
            Some(slots_sender),
            Some(processed_tx),
            Some(CommitmentLevel::Processed),
        );

        let confirmed_future = GrpcClient::subscribe(
            GrpcClient::create_client(addr.clone()).await?,
            None,
            Some(confirmed_tx),
            Some(CommitmentLevel::Confirmed),
        );

        let finalized_future = GrpcClient::subscribe(
            GrpcClient::create_client(addr).await?,
            None,
            Some(finalized_tx),
            Some(CommitmentLevel::Finalized),
        );

        tokio::select! {
            res = processed_future => {
                bail!("Processed stream closed unexpectedly {res:?}");
            }
            res = confirmed_future => {
                bail!("Confirmed stream closed unexpectedly {res:?}");
            }
            res = finalized_future => {
                bail!("Finalized stream closed unexpectedly {res:?}");
            }
        }
    }
}
