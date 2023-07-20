use std::collections::HashMap;

use anyhow::{bail, Context};

use futures::StreamExt;
use solana_sdk::slot_history::Slot;
use tokio::sync::mpsc::UnboundedSender;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::{
    prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeUpdateTransaction,
    },
    tonic::service::Interceptor,
};

pub struct GrpcClient;

impl GrpcClient {
    pub async fn subscribe<F: Interceptor>(
        client: &mut GeyserGrpcClient<F>,
        slots_sender: Option<UnboundedSender<Slot>>,
        tx_sender: Option<UnboundedSender<SubscribeUpdateTransaction>>,
        commitment: Option<CommitmentLevel>,
    ) -> anyhow::Result<()> {
        // check if atomic_slot is None
        // subscribe to slot only if atomic_slot is Some
        let mut slots = HashMap::new();
        if slots_sender.is_some() {
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
                commitment,
                Default::default(),
            )
            .await?;

        while let Some(message) = stream.next().await {
            let message = message?;

            let Some(update) = message.update_oneof else {
                continue;
            };

            match update {
                UpdateOneof::Slot(slot) => {
                    let Some(slots_sender) = slots_sender.as_ref() else {
                        bail!("Unexpected slot notification");
                    };

                    slots_sender
                        .send(slot.slot)
                        .context("Grpc slot sender closed")?;
                }
                UpdateOneof::Transaction(tx) => {
                    let Some(tx_sender) = tx_sender.as_ref() else {
                        bail!("Unexpected slot notification");
                    };

                    tx_sender.send(tx).context("Grpc tx sender closed")?;
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
