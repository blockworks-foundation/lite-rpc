use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use log::{debug, error, info};
use serde::Serializer;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::clock::Slot;
use tokio::select;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::broadcast::error::TryRecvError;
use tokio::sync::RwLock;
use yellowstone_grpc_proto::geyser::CommitmentLevel;

use solana_lite_rpc_cluster_endpoints::grpc_subscription::create_block_processing_task;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;

pub const GRPC_VERSION: &str = "1.16.1";

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
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

    start_progressor("green".to_string(), blocks_notifier_green, rx_tip.clone(), offer_block_sender.clone());
    start_progressor("blue".to_string(), blocks_notifier_blue, rx_tip.clone(), offer_block_sender.clone());

    // test
    tokio::spawn(async move {
        // need to wait until channels reached slot beyond tip
        // tokio::time::sleep(Duration::from_secs(14)).await;

        let mut current_tip = 0;
        loop {
            let slot_offered = offer_block_notifier.recv().await.unwrap();
            info!("<< offered slot: {:?}", slot_offered);

            // do a dump move and send it back as tip
            let OfferBlockMsg::NextSlot(_label, slot_offered) = slot_offered;
            let new_tip = slot_offered;
            tx_tip.send(new_tip).unwrap();
            info!("--> progressing tip to {}", new_tip);
        }


    });

    sleep(Duration::from_secs(1000));

}

#[derive(Debug)]
enum OfferBlockMsg {
    NextSlot(String, Slot),
}

fn start_progressor(label: String, blocks_notifier: Receiver<ProducedBlock>, mut rx_tip: tokio::sync::watch::Receiver<Slot>,
                    offer_block_sender: tokio::sync::mpsc::Sender<OfferBlockMsg>) {
    tokio::spawn(async move {
        let mut blocks_notifier = blocks_notifier.resubscribe();
        // for test only
        // let start_slot = blocks_notifier.recv().await.unwrap().slot;

        let mut tip = 0;
        info!("starting at tip {}", tip);

        // block after tip offered by this stream
        let mut block_after_tip: Slot = 0;
        'main_loop: loop {
            select! {
                _ = rx_tip.changed() => {
                    tip = rx_tip.borrow().clone();
                    info!("++> {} tip changed to {}", label, tip);
                }
                recv_result = blocks_notifier.recv(), if !(block_after_tip > tip) => {
                    match recv_result {
                        Ok(block) => {
                            info!("=> recv on {}: {}",label, format_block(&block));
                            if block.slot > tip {
                                info!("==> {}: beyond tip ({} > {})", label, block.slot, tip);
                                block_after_tip = block.slot;
                                offer_block_sender.send(OfferBlockMsg::NextSlot(label.clone(), block_after_tip)).await.unwrap();
                                // this thread will sleep and not issue any recvs until we get tip.changed signal
                                continue 'main_loop;
                            }
                        }
                        Err(e) => {
                            // TODO what to do?
                            error!("Error receiving block: {}", e);
                            continue 'main_loop;
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
                info!("% delay value");
                tokio::time::sleep(Duration::from_millis(700)).await;
            }
            if counter % 5 == 0 {
                info!("% drop value");
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
