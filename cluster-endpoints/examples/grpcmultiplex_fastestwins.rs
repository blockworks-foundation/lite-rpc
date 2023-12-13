use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::ops::{Add, Deref, Sub};
use std::pin::{pin, Pin};
use anyhow::{bail, Context};
use async_stream::stream;
use futures::{Stream, StreamExt};
use itertools::{Itertools};

use log::{debug, error, info, warn};
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::{select};
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, Duration, timeout, Instant, sleep_until};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta, SubscribeUpdate, SubscribeUpdateBlock, SubscribeUpdateBlockMeta};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::tonic::transport::ClientTlsConfig;

use solana_lite_rpc_cluster_endpoints::grpc_subscription::{create_block_processing_task, map_produced_block};
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;

#[tokio::main]
pub async fn main() {
    // RUST_LOG=info,grpc_using_streams=debug
    tracing_subscriber::fmt::init();
    // console_subscriber::init();

    // mango validator (mainnet)
    let grpc_addr_mainnet_triton = "http://202.8.9.108:10000".to_string();
    // via toxiproxy
    let grpc_addr_mainnet_triton_toxi = "http://127.0.0.1:10001".to_string();
    // ams81 (mainnet)
    let grpc_addr_mainnet_ams81 = "http://202.8.8.12:10000".to_string();
    // testnet - NOTE: this connection has terrible lags (almost 5 minutes)
    // let grpc_addr = "http://147.28.169.13:10000".to_string();

    let (block_sx, blocks_notifier) = tokio::sync::broadcast::channel(1000);

    let green_config = GrpcSourceConfig::new("green".to_string(), grpc_addr_mainnet_triton, None);
    let blue_config = GrpcSourceConfig::new("blue".to_string(), grpc_addr_mainnet_ams81, None);
    let toxiproxy_config = GrpcSourceConfig::new("toxiproxy".to_string(), grpc_addr_mainnet_triton_toxi, None);

    create_multiplex(
        vec![green_config, blue_config, toxiproxy_config],
        CommitmentLevel::Confirmed,
        block_sx);

    start_example_consumer(blocks_notifier);

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;

}

fn start_example_consumer(blocks_notifier: Receiver<ProducedBlock>) {
    tokio::spawn(async move {
        let mut blocks_notifier = blocks_notifier;
        loop {
            let block = blocks_notifier.recv().await.unwrap();
            info!("received block #{} with {} txs", block.slot, block.transactions.len());
        }
    });
}

fn create_multiplex(
    grpc_sources: Vec<GrpcSourceConfig>,
    commitment_level: CommitmentLevel,
    block_sx: Sender<ProducedBlock>,
) -> AnyhowJoinHandle {
    assert!(
        commitment_level == CommitmentLevel::Confirmed
            || commitment_level == CommitmentLevel::Finalized,
        "Only CONFIRMED and FINALIZED is supported");
    // note: PROCESSED blocks are not sequential in presense of forks; this will break the logic

    if grpc_sources.len() < 1 {
        panic!("Must have at least one source");
    }

    let commitment_config = match commitment_level {
        CommitmentLevel::Confirmed => CommitmentConfig::confirmed(),
        CommitmentLevel::Finalized => CommitmentConfig::finalized(),
        // not used, not supported!
        CommitmentLevel::Processed => CommitmentConfig::processed(),
    };

    let jh = tokio::spawn(async move {
        info!("Starting multiplexer with {} sources: {}",
            grpc_sources.len(),
            grpc_sources.iter().map(|source| source.label.clone()).join(", "));

        let mut futures = futures::stream::SelectAll::new();
        for grpc_source in grpc_sources {
            // note: stream never terminates
            let stream = create_geyser_reconnecting_stream(grpc_source.clone(), commitment_level).await;
            futures.push(Box::pin(stream));
        }

        let mut current_slot: Slot = 0;

        let mut start_stream33 = false;
        'main_loop: loop {

            let block_cmd = select! {
                message = futures.next() => {
                    match message {
                        Some(message) => {
                            map_filter_block_message(current_slot, message, commitment_config)
                        }
                        None => {
                            panic!("source stream is not supposed to terminate");
                        }
                    }
                }
            };

            match block_cmd {
                BlockCmd::ForwardBlock(block) => {
                    current_slot = block.slot;
                    block_sx.send(block).context("send block to downstream")?;
                }
                BlockCmd::DiscardBlockBehindTip(slot) => {
                    debug!(". discarding redundant block #{}", slot);
                }
                BlockCmd::SkipMessage => {
                    debug!(". skipping this message by type");
                }
            }

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

fn map_filter_block_message(current_slot: Slot, update_message: SubscribeUpdate, commitment_config: CommitmentConfig) -> BlockCmd {
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

#[derive(Clone, Debug)]
struct GrpcSourceConfig {
    // symbolic name used in logs
    label: String,
    grpc_addr: String,
    grpc_x_token: Option<String>,
    tls_config: Option<ClientTlsConfig>,
}

impl GrpcSourceConfig {
    fn new(label: String, grpc_addr: String, grpc_x_token: Option<String>) -> Self {
        Self {
            label,
            grpc_addr,
            grpc_x_token,
            tls_config: None,
        }
    }
}

// TODO use GrpcSource
// note: stream never terminates
async fn create_geyser_reconnecting_stream(
    grpc_source: GrpcSourceConfig,
    commitment_level: CommitmentLevel) -> impl Stream<Item = SubscribeUpdate> {
    let label = grpc_source.label.clone();
    stream! {
        let mut throttle_barrier = Instant::now();
        'reconnect_loop: loop {
            sleep_until(throttle_barrier).await;
            throttle_barrier = Instant::now().add(Duration::from_millis(1000));

            let connect_result = GeyserGrpcClient::connect_with_timeout(
                grpc_source.grpc_addr.clone(), grpc_source.grpc_x_token.clone(), grpc_source.tls_config.clone(),
                Some(Duration::from_secs(2)), Some(Duration::from_secs(2)), false).await;

            let mut client = match connect_result {
                Ok(connected_client) => connected_client,
                Err(geyser_grpc_client_error) => {
                    // TODO identify non-recoverable errors and cancel stream
                    warn!("Connect failed on {} - retrying: {:?}", label, geyser_grpc_client_error);
                    continue 'reconnect_loop;
                }
            };

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

            let subscribe_result = client
                .subscribe_once(
                    HashMap::new(),
                    Default::default(),
                    HashMap::new(),
                    Default::default(),
                    blocks_subs,
                    Default::default(),
                    Some(commitment_level),
                    Default::default(),
                    None,
                ).await;

            let geyser_stream = match subscribe_result {
                Ok(subscribed_stream) => subscribed_stream,
                Err(geyser_grpc_client_error) => {
                    // TODO identify non-recoverable errors and cancel stream
                    warn!("Subscribe failed on {} - retrying: {:?}", label, geyser_grpc_client_error);
                    continue 'reconnect_loop;
                }
            };

            for await update_message in geyser_stream {
                match update_message {
                    Ok(update_message) => {
                        info!(">message on {}", label);
                        yield update_message;
                    }
                    Err(tonic_status) => {
                        // TODO identify non-recoverable errors and cancel stream
                        warn!("Receive error on {} - retrying: {:?}", label, tonic_status);
                        continue 'reconnect_loop;
                    }
                }
            } // -- production loop

            warn!("stream consumer loop terminated for {}", label);
        } // -- main loop
    } // -- stream!

}
