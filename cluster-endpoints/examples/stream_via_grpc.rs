use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use futures::StreamExt;
use itertools::{ExactlyOneError, Itertools};

use log::{debug, error, info, warn};
use serde::Serializer;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::clock::Slot;
use tokio::select;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration, timeout};
use yellowstone_grpc_proto::geyser::CommitmentLevel;

use solana_lite_rpc_cluster_endpoints::grpc_subscription::create_block_processing_task;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;

pub const GRPC_VERSION: &str = "1.16.1";

#[tokio::main]
// #[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() {
    // info,solana_lite_rpc_cluster_endpoints=debug,stream_via_grpc=trace
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
    let (block_sx_green, blocks_notifier_green) = start_monkey_broadcast::<ProducedBlock>(1000);
    let (block_sx_blue, blocks_notifier_blue) = tokio::sync::broadcast::channel(1000);

    let grpc_x_token = None;
    let block_confirmed_task_green: AnyhowJoinHandle = create_block_processing_task(
        _grpc_addr_mainnet_triton.clone(),
        grpc_x_token.clone(),
        block_sx_green.clone(),
        CommitmentLevel::Confirmed,
    );


    let block_confirmed_task_blue: AnyhowJoinHandle = create_block_processing_task(
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


    let (tx_tip, mut rx_tip) = tokio::sync::watch::channel::<Slot>(0);

    let (offer_block_sender, mut offer_block_notifier) = tokio::sync::mpsc::channel::<OfferBlockMsg>(100);

    start_progressor("green".to_string(), blocks_notifier_green, rx_tip.clone(), offer_block_sender.clone()).await;
    start_progressor("blue".to_string(), blocks_notifier_blue, rx_tip.clone(), offer_block_sender.clone()).await;


    // test
    tokio::spawn(async move {
        // need to wait until channels reached slot beyond tip
        // tokio::time::sleep(Duration::from_secs(14)).await;

        // see also start procedure!
        let mut current_tip = 0;
        let mut blocks_offered = Vec::<BlockRef>::new();
        loop {

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
                            assert_ne!(current_tip, 0, "must not see empty tip");
                            tx_tip.send(current_tip).unwrap();
                            blocks_offered.clear();
                            continue;
                        }

                        blocks_offered.push(block_offered);

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

                    if current_tip == 0 {
                        let start_slot = blocks_offered.iter().max_by(|lhs,rhs| lhs.slot.cmp(&rhs.slot)).expect("need at least one slot to start");
                        current_tip = start_slot.slot;
                        assert_ne!(current_tip, 0, "must not see empty tip");
                        tx_tip.send(current_tip).unwrap();
                        info!("--> starting with tip {}", current_tip);
                        blocks_offered.clear();
                        continue;
                    }

                    match blocks_offered.iter().filter(|b| b.parent_slot == current_tip).exactly_one() {
                        Ok(found_next) => {
                            current_tip = found_next.slot;
                            assert_ne!(current_tip, 0, "must not see empty tip");
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

    });


    // emitter
    tokio::spawn(async move {
        let mut rx_tip = rx_tip;
        loop {
            let tip = *rx_tip.borrow_and_update();
            if tip != 0 {
                info!("<<<< emit block: {}", tip);
            }
            if rx_tip.changed().await.is_err() {
                break;
            }
        }
    });

    // "infinite" sleep
    sleep(Duration::from_secs(1800)).await;

    info!("Shutting down...");
    info!("...tip variable");
    drop(tx_tip);
    info!("Shutdown completed.");

}

#[derive(Clone, Debug)]
struct BlockRef {
    pub slot: Slot,
    pub parent_slot: Slot,
}

impl From<ProducedBlock> for BlockRef {
    fn from(block: ProducedBlock) -> Self {
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

async fn start_progressor(label: String, blocks_notifier: Receiver<ProducedBlock>, mut rx_tip: tokio::sync::watch::Receiver<Slot>,
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

fn format_block(block: &ProducedBlock) -> String {
    format!("{:?}@{} (-> {})", block.slot, block.commitment_config.commitment, block.parent_slot)
}


fn start_monkey_broadcast<T: Clone + Send + 'static>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, monkey_upstream) = tokio::sync::broadcast::channel::<T>(1024);
    let (monkey_downstream, rx) = tokio::sync::broadcast::channel::<T>(capacity);

    tokio::spawn(async move {
        let mut monkey_upstream = monkey_upstream;
        'recv_loop: for counter in 0.. {
            let value = monkey_upstream.recv().await.unwrap();
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
