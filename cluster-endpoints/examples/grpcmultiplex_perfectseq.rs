use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use anyhow::{bail, Context};
use futures::StreamExt;
use itertools::{ExactlyOneError, Itertools};

use log::{debug, error, info, warn};
use serde::Serializer;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::select;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration, timeout};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::{CommitmentLevel, SubscribeRequestFilterBlocksMeta, SubscribeUpdateBlockMeta};
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;

use solana_lite_rpc_cluster_endpoints::grpc_subscription::create_block_processing_task;
use solana_lite_rpc_core::AnyhowJoinHandle;

pub const GRPC_VERSION: &str = "1.16.1";

#[tokio::main]
// #[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() {
    // RUST_LOG=info,stream_via_grpc=debug,drain_to_tip_pattern=debug
    tracing_subscriber::fmt::init();


    // let rpc_client = Arc::new(RpcClient::new("http://127.0.0.0:8899".to_string()));
    let rpc_client = Arc::new(RpcClient::new("http://api.mainnet-beta.solana.com/".to_string()));
    // mango validator (mainnet)
    let _grpc_addr_mainnet_triton = "http://202.8.9.108:10000".to_string();
    // ams81 (mainnet)
    let grpc_addr_mainnet_ams81 = "http://202.8.8.12:10000".to_string();
    // testnet - NOTE: this connection has terrible lags (almost 5 minutes)
    // let grpc_addr = "http://147.28.169.13:10000".to_string();

    // let (block_sx_green, blocks_notifier_green) = tokio::sync::broadcast::channel(1000);
    let (block_sx_green, blocks_notifier_green) = start_monkey_broadcast::<SimpleBlockMeta>(1000);
    let (block_sx_blue, blocks_notifier_blue) =  tokio::sync::broadcast::channel(1000);

    // TODO ship ProducedBlock
    let (sx_multi, mut rx_multi) = tokio::sync::broadcast::channel::<Slot>(1000);

    let grpc_x_token = None;
    let block_confirmed_task_green: AnyhowJoinHandle = create_blockmeta_processing_task(
        _grpc_addr_mainnet_triton.clone(),
        grpc_x_token.clone(),
        block_sx_green.clone(),
        CommitmentLevel::Confirmed,
    );

    let block_confirmed_task_blue: AnyhowJoinHandle = create_blockmeta_processing_task(
        grpc_addr_mainnet_ams81.clone(),
        grpc_x_token.clone(),
        block_sx_blue.clone(),
        CommitmentLevel::Confirmed,
    );
    // let block_finalized_task: AnyhowJoinHandle = create_block_processing_task(
    //     grpc_addr,
    //     grpc_x_token,
    //     block_sx,
    //     CommitmentLevel::Finalized,
    // );


    let (tx_tip, _) = tokio::sync::watch::channel::<Slot>(0);

    let (offer_block_sender, mut offer_block_notifier) = tokio::sync::mpsc::channel::<OfferBlockMsg>(100);

    // producers
    start_progressor("green".to_string(), blocks_notifier_green, tx_tip.subscribe(), offer_block_sender.clone()).await;
    start_progressor("blue".to_string(), blocks_notifier_blue, tx_tip.subscribe(), offer_block_sender.clone()).await;

    // merge task
    // collect the offered slots from the two channels
    tokio::spawn(async move {
        // need to wait until channels reached slot beyond tip
        // tokio::time::sleep(Duration::from_secs(14)).await;

        // see also start procedure!
        let mut current_tip = 0;
        let mut blocks_offered = Vec::<BlockRef>::new();
        'main_loop: loop {

            // note: we abuse the timeout mechanism to collect some blocks
            let timeout_secs = if current_tip == 0 { 3 } else { 10 };

            let msg_or_timeout = timeout(Duration::from_secs(timeout_secs), offer_block_notifier.recv()).await;
            info!("- current_tip={}", current_tip);

            match msg_or_timeout {
                Ok(Some(offer_block_msg)) => {
                    // collect the offered slots from the channels
                    if let OfferBlockMsg::NextSlot(label, block_offered) = offer_block_msg {
                        info!("<< offered slot from {}: {:?}", label, block_offered);

                        // TOOD use .parent instead
                        if block_offered.parent_slot == current_tip {
                            current_tip = block_offered.slot;
                            info!("<< take block from {} as new tip {}", label, current_tip);
                            assert_ne!(current_tip, 0, "must not see uninitialized tip");

                            emit_block_on_multiplex_output_channel(&sx_multi, current_tip);
                            tx_tip.send(current_tip).unwrap();
                            blocks_offered.clear();
                            continue 'main_loop;
                        } else {
                            // keep the block for later
                            blocks_offered.push(block_offered);
                            continue 'main_loop;
                        }


                    }
                    // TODO handle else
                }
                Ok(None) => {
                    // TODO double-check
                    // channel closed
                    info!("--> channel closed");
                    break;
                }
                Err(_elapsed) => {
                    // timeout
                    info!("--> timeout: got these slots: {:?}", blocks_offered);

                    // we abuse timeout feature to wait for some blocks to arrive to select the "best" one
                    if current_tip == 0 {
                        let start_slot = blocks_offered.iter().max_by(|lhs,rhs| lhs.slot.cmp(&rhs.slot)).expect("need at least one slot to start");
                        current_tip = start_slot.slot;
                        assert_ne!(current_tip, 0, "must not see uninitialized tip");

                        emit_block_on_multiplex_output_channel(&sx_multi, current_tip);
                        tx_tip.send(current_tip).unwrap();
                        info!("--> initializing with tip {}", current_tip);
                        blocks_offered.clear();
                        continue 'main_loop;
                    }

                    match blocks_offered.iter().filter(|b| b.parent_slot == current_tip).exactly_one() {
                        Ok(found_next) => {
                            current_tip = found_next.slot;
                            assert_ne!(current_tip, 0, "must not see uninitialized tip");
                            tx_tip.send(current_tip).unwrap();
                            info!("--> progressing tip to {}", current_tip);
                            blocks_offered.clear();
                        }
                        Err(missed) => {
                            warn!("--> no slots offered - SHOULD ABORT - no hope to progress");
                        }
                    }

                    sleep(Duration::from_millis(500)).await;
                }
            }
        } // -- recv loop

        info!("Shutting down merge task.");
    });


    tokio::spawn(async move {
        let mut rx_multi = rx_multi;
        let mut last_slot = 0;
        loop {
            let slot = rx_multi.recv().await.unwrap();
            assert_ne!(slot, 0, "must not see empty slot");
            info!("==> multiplexed slot: {}", slot);
            if slot - last_slot > 1 && last_slot != 0 {
                warn!("==> gap: {} -> {}", last_slot, slot);
            }
            last_slot = slot;
        }
    });


    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;

    // TODO proper shutdown
    info!("Shutdown completed.");

}

fn emit_block_on_multiplex_output_channel(sx_multi: &Sender<Slot>, mut current_tip: u64) {
    sx_multi.send(current_tip).unwrap();
}

#[derive(Clone, Debug)]
struct BlockRef {
    pub slot: Slot,
    pub parent_slot: Slot,
}

impl From<SimpleBlockMeta> for BlockRef {
    fn from(block: SimpleBlockMeta) -> Self {
        BlockRef {
            slot: block.slot,
            parent_slot: block.parent_slot,
        }
    }
}

#[derive(Debug)]
enum OfferBlockMsg {
    NextSlot(String, BlockRef),
}

async fn start_progressor(label: String, blocks_notifier: Receiver<SimpleBlockMeta>, mut rx_tip: tokio::sync::watch::Receiver<Slot>,
                    offer_block_sender: tokio::sync::mpsc::Sender<OfferBlockMsg>) {
    tokio::spawn(async move {
        // TODO is .resubscribe what we want?
        let mut blocks_notifier = blocks_notifier.resubscribe();
        // for test only
        // let start_slot = blocks_notifier.recv().await.unwrap().slot;

        // local copy of tip
        let mut local_tip = 0;

        // block after tip offered by this stream
        // TODO: block_after_tip is only valid/useful if greater than tip
        let mut highest_block: BlockRef = BlockRef {
            slot: 0,
            parent_slot: 0,
        };
        'main_loop: loop {
            select! {
                result = rx_tip.changed() => {
                    if result.is_err() {
                        debug!("Tip variable closed for {}", label);
                        break 'main_loop;
                    }
                    local_tip = rx_tip.borrow_and_update().clone();
                    info!("++> {} tip changed to {}", label, local_tip);
                    // TODO update local tip
                }
                recv_result = blocks_notifier.recv(), if !(highest_block.slot > local_tip) => {
                    match recv_result {
                        Ok(block) => {
                            info!("=> recv on {}: {}",label, format_block(&block));
                            if block.slot > local_tip {
                                info!("==> {}: beyond tip ({} > {})", label, block.slot, local_tip);
                                highest_block = BlockRef::from(block);
                                offer_block_sender.send(OfferBlockMsg::NextSlot(label.clone(), highest_block.clone())).await.unwrap();
                                // this thread will sleep and not issue any recvs until we get tip.changed signal
                                continue 'main_loop;
                            }
                        }
                        Err(e) => {
                            // TODO what to do?
                            error!("Error receiving block: {}", e);
                            break 'main_loop;
                        }
                    }
                }
            }
        }
    });
}

fn format_block(block: &SimpleBlockMeta) -> String {
    format!("{:?}@{} (-> {})", block.slot, block.commitment_config.commitment, block.parent_slot)
}


fn start_monkey_broadcast<T: Clone + Send + 'static>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, monkey_upstream) = tokio::sync::broadcast::channel::<T>(1024);
    let (monkey_downstream, rx) = tokio::sync::broadcast::channel::<T>(capacity);

    tokio::spawn(async move {
        let mut monkey_upstream = monkey_upstream;
        'recv_loop: for counter in 1.. {
            let value = match monkey_upstream.recv().await {
                Ok(msg) => {
                    msg
                }
                Err(RecvError::Closed) => {
                    return ();
                }
                Err(RecvError::Lagged(_)) => {
                    continue;
                }
            };
            if let Ok(val) = monkey_upstream.recv().await {
                val
            } else {
                return ();
            };
            // info!("forwarding: {}", value);
            // failes if there are no receivers

            if counter % 3 == 0 {
                debug!("% delay value");
                tokio::time::sleep(Duration::from_millis(700)).await;
            }
            if counter % 5 == 0 {
                debug!("% drop value");
                continue 'recv_loop;
            }
            if counter % 23 == 0 {
                debug!("% system outage + reboot");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            let send_result = monkey_downstream.send(value);
            match send_result {
                Ok(_) => {
                    debug!("% forwarded");
                }
                Err(_) => panic!("Should never happen")
            }
        }
    });

    (tx, rx)
}

#[derive(Debug, Clone)]
struct SimpleBlockMeta {
    slot: Slot,
    parent_slot: Slot,
    commitment_config: CommitmentConfig,
}

fn create_blockmeta_processing_task(
    grpc_addr: String,
    grpc_x_token: Option<String>,
    block_sx: Sender<SimpleBlockMeta>,
    commitment_level: CommitmentLevel,
) -> AnyhowJoinHandle {
    let mut blocks_subs = HashMap::new();
    blocks_subs.insert(
        "client".to_string(),
        SubscribeRequestFilterBlocksMeta {},
    );

    let commitment_config = match commitment_level {
        CommitmentLevel::Confirmed => CommitmentConfig::confirmed(),
        CommitmentLevel::Finalized => CommitmentConfig::finalized(),
        CommitmentLevel::Processed => CommitmentConfig::processed(),
    };


    tokio::spawn(async move {
        // connect to grpc
        let mut client = GeyserGrpcClient::connect(grpc_addr, grpc_x_token, None)?;
        let mut stream = client
            .subscribe_once(
                HashMap::new(),
                Default::default(),
                HashMap::new(),
                Default::default(),
                Default::default(),
                blocks_subs,
                Some(commitment_level),
                Default::default(),
                None,
            )
            .await?;

        while let Some(message) = stream.next().await {
            let message = message?;

            let Some(update) = message.update_oneof else {
                continue;
            };

            match update {
                UpdateOneof::BlockMeta(block_meta) => {
                    let block_meta = process_blockmeta(block_meta, commitment_config);
                    block_sx
                        .send(block_meta)
                        .context("Grpc failed to send a block")?;
                }
                UpdateOneof::Ping(_) => {
                    log::trace!("GRPC Ping");
                }
                u => {
                    bail!("Unexpected update: {u:?}");
                }
            };
        }
        bail!("geyser slot stream ended");
    })
}

fn process_blockmeta(block_meta: SubscribeUpdateBlockMeta, commitment_config: CommitmentConfig) -> SimpleBlockMeta {
    SimpleBlockMeta {
        slot: block_meta.slot,
        parent_slot: block_meta.parent_slot,
        commitment_config: commitment_config,
    }
}


