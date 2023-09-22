use crate::endpoint_stremers::EndpointStreaming;
use anyhow::Context;
use solana_lite_rpc_core::{commitment_utils::Commitment, AnyhowJoinHandle};
use solana_sdk::slot_history::Slot;
use std::collections::BTreeSet;

const NB_BLOCKS_TO_CACHE: usize = 1024;

pub fn multiplexing_endstreams(
    rpc_endpoints: EndpointStreaming,
    grpc_endpoint: EndpointStreaming,
) -> anyhow::Result<(EndpointStreaming, Vec<AnyhowJoinHandle>)> {
    let (slot_sx, slot_notifier) = tokio::sync::broadcast::channel(10);
    let (block_sx, blocks_notifier) = tokio::sync::broadcast::channel(10);
    let mut endpoint_tasks = vec![];

    let mut rpc_slot_notifier = rpc_endpoints.slot_notifier;
    let mut grpc_slot_notifier = grpc_endpoint.slot_notifier;
    let slot_multiplexer: AnyhowJoinHandle = tokio::spawn(async move {
        let mut processed_slot = 0;
        let mut estimated_slot = 0;
        loop {
            let notification = tokio::select! {
                rpc_slot = rpc_slot_notifier.recv() => {
                    if let Ok(slot_notification) = rpc_slot {
                        slot_notification
                    } else {
                        continue;
                    }
                },
                grpc_slot = grpc_slot_notifier.recv() => {
                    if let Ok(slot_notification) = grpc_slot {
                        slot_notification
                    } else {
                        continue;
                    }
                }
            };

            if notification.processed_slot > processed_slot
                || notification.estimated_processed_slot > estimated_slot
            {
                processed_slot = notification.processed_slot;
                estimated_slot = notification.estimated_processed_slot;
                slot_sx.send(notification).context("send channel broken")?;
            }
        }
    });

    let mut rpc_block_notifier = rpc_endpoints.blocks_notifier;
    let mut grpc_block_notifier = grpc_endpoint.blocks_notifier;
    let block_multiplexer: AnyhowJoinHandle = tokio::spawn(async move {
        let mut block_notified = BTreeSet::<(Slot, Commitment)>::new();
        loop {
            let block = tokio::select! {
                block_notification = rpc_block_notifier.recv() => {
                    if let Ok(block) = block_notification {
                        block
                    } else  {
                        continue;
                    }
                },
                block_notification = grpc_block_notifier.recv() => {
                    if let Ok(block) = block_notification {
                        block
                    } else  {
                        continue;
                    }
                }
            };
            let key = (block.slot, block.commitment_config.into());
            if block_notified.contains(&key) {
                block_notified.insert(key);
                if block_notified.len() > NB_BLOCKS_TO_CACHE {
                    block_notified.pop_first();
                }
                block_sx.send(block).context("send channel broken")?;
            }
        }
    });

    endpoint_tasks.push(slot_multiplexer);
    endpoint_tasks.push(block_multiplexer);

    let streamers = EndpointStreaming {
        blocks_notifier,
        slot_notifier,
        cluster_info_notifier: rpc_endpoints.cluster_info_notifier.resubscribe(),
        vote_account_notifier: rpc_endpoints.vote_account_notifier.resubscribe(),
    };
    Ok((streamers, endpoint_tasks))
}
