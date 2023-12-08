use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;
use std::thread;
use anyhow::{bail, Context};
use async_stream::stream;
use futures::{pin_mut, Stream, StreamExt};
use itertools::{ExactlyOneError, Itertools};

use log::{debug, error, info, warn};
use serde::Serializer;
use serde_json::de::Read;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::{select};
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration, timeout};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta, SubscribeUpdate, SubscribeUpdateBlock, SubscribeUpdateBlockMeta};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::tonic::Status;

use solana_lite_rpc_cluster_endpoints::grpc_subscription::{create_block_processing_task, map_produced_block};
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;

#[tokio::main]
// #[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() {
    // RUST_LOG=info,grpc_using_streams=debug
    tracing_subscriber::fmt::init();


    // mango validator (mainnet)
    let grpc_addr_mainnet_triton = "http://202.8.9.108:10000".to_string();
    // ams81 (mainnet)
    let grpc_addr_mainnet_ams81 = "http://202.8.8.12:10000".to_string();
    // testnet - NOTE: this connection has terrible lags (almost 5 minutes)
    // let grpc_addr = "http://147.28.169.13:10000".to_string();


    let (block_sx, blocks_notifier) = tokio::sync::broadcast::channel(1000);
    create_multiplex(grpc_addr_mainnet_triton, grpc_addr_mainnet_ams81, block_sx).await;

    tokio::spawn(async move {
        let mut blocks_notifier = blocks_notifier;
        loop {
            let block = blocks_notifier.recv().await.unwrap();
            info!("received block #{} with {} txs", block.slot, block.transactions.len());
        }
    });


    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;

}

async fn create_multiplex(
    grpc_addr_mainnet_triton: String,
    grpc_addr_mainnet_ams81: String,
    block_sx: Sender<ProducedBlock>,
) -> JoinHandle<()> {

    // TODO
    let commitment_config = CommitmentConfig::confirmed();

    let jh = tokio::spawn(async move {

        let mut green = create_geyser_stream2(grpc_addr_mainnet_triton.clone(), None).await;
        let mut blue = create_geyser_stream2(grpc_addr_mainnet_ams81.clone(), None).await;
        pin_mut!(green);
        pin_mut!(blue);

        let mut current_slot = 0 as Slot;

        'main_loop: loop {

            let block_cmd =
                select!(
                    message = green.next() => {
                        match message {
                            Some(message) => {
                                map_filter_block_message(current_slot, message, commitment_config)
                            }
                            None => {
                                panic!("must not close the stream");
                            }
                        }

                    },
                    message = blue.next() => {
                       match message {
                            Some(message) => {
                                map_filter_block_message(current_slot, message, commitment_config)
                            }
                            None => {
                                panic!("must not close the stream");
                            }
                        }
                    }
                );

            match block_cmd {
                BlockCmd::ForwardBlock(block) => {
                    current_slot = block.slot;
                    block_sx.send(block).unwrap();
                }
                BlockCmd::DiscardBlockBehindTip(slot) => {
                    debug!("Discarding redundand block #{}", slot);
                }
                BlockCmd::SkipMessage => {
                    debug!("Skipping this message by type");
                }
            }

            sleep(Duration::from_millis(500)).await;

        }
    });

    return jh;
}

#[derive(Debug)]
enum BlockCmd {
    ForwardBlock(ProducedBlock),
    DiscardBlockBehindTip(Slot),
    // skip geyser messages which are not block related updates
    SkipMessage,
}

fn map_filter_block_message(current_slot: Slot, update_message: SubscribeUpdate, commitment_config: CommitmentConfig,) -> BlockCmd {
    if let Some(UpdateOneof::Block(update_block_message)) = update_message.update_oneof {
        if update_block_message.slot <= current_slot && current_slot != 0 {
            // no progress - skip this
            return BlockCmd::DiscardBlockBehindTip(update_block_message.slot);
        }

        // expensive
        let produced_block = map_produced_block(update_block_message, commitment_config);

        BlockCmd::ForwardBlock(produced_block)
    } else {
        return BlockCmd::SkipMessage;
    }

}

async fn create_geyser_stream(grpc_addr: String, x_token: Option<String>) -> impl Stream<Item = Result<SubscribeUpdate, Status>> {
    let mut client = GeyserGrpcClient::connect(grpc_addr, x_token, None).unwrap();

    let mut blocks_subs = HashMap::new();
    blocks_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterBlocks {
            account_include: Default::default(),
            include_transactions: Some(true),
            include_accounts: Some(false),
            include_entries: Some(false),
        },
    );

    let stream = client
        .subscribe_once(
            HashMap::new(),
            Default::default(),
            HashMap::new(),
            Default::default(),
            blocks_subs,
            Default::default(),
            Some(CommitmentLevel::Confirmed),
            Default::default(),
            None,
        ).await.unwrap();
    // TODO fix unwrap

    // TODO pull tonic error handling inside this method
    return stream;
}

async fn create_geyser_stream2(grpc_addr: String, x_token: Option<String>) -> impl Stream<Item = SubscribeUpdate> {


    let mut client = GeyserGrpcClient::connect(grpc_addr, x_token, None).unwrap();

    let mut blocks_subs = HashMap::new();
    blocks_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterBlocks {
            account_include: Default::default(),
            include_transactions: Some(true),
            include_accounts: Some(false),
            include_entries: Some(false),
        },
    );

    let stream = client
        .subscribe_once(
            HashMap::new(),
            Default::default(),
            HashMap::new(),
            Default::default(),
            blocks_subs,
            Default::default(),
            Some(CommitmentLevel::Confirmed),
            Default::default(),
            None,
        ).await.unwrap();

    stream! {

       for await update_message in stream {

            match update_message {
                Ok(update_message) => {
                    yield update_message;
                }
                Err(status) => {
                    error!(">stream: {:?}", status);
                }
            }

        }

    }



}
