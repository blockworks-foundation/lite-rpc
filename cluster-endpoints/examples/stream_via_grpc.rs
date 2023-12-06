use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use log::{error, info};
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
use crate::CmdNext::DeleteSourceAtIndex;

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

    let (block_sx_green, blocks_notifier_green) = tokio::sync::broadcast::channel(1000);
    let (block_sx_blue, blocks_notifier_blue) = tokio::sync::broadcast::channel(1000);

    let grpc_x_token = None;
    let block_confirmed_task_green_in: AnyhowJoinHandle = create_block_processing_task(
        grpc_addr_mainnet_ams81.clone(),
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


    start_progressor("green".to_string(), blocks_notifier_green, rx_tip.clone());

    sleep(Duration::from_secs(1000));

}

fn start_progressor(label: String, blocks_notifier: Receiver<ProducedBlock>, mut rx_tip: tokio::sync::watch::Receiver<Slot>) {
    tokio::spawn(async move {
        let mut blocks_notifier = blocks_notifier.resubscribe();
        // for test only
        let start_slot = blocks_notifier.recv().await.unwrap().slot;

        let mut tip = start_slot + 20;
        info!("starting at tip {}", tip);

        // block after tip offered by this stream
        let mut block_after_tip: Slot = 0;
        'main_loop: loop {
            select! {
                _ = rx_tip.changed() => {
                    tip = rx_tip.borrow().clone();
                    info!("tip changed to {}", tip);
                }
                recv_result = blocks_notifier.recv(), if !(block_after_tip > tip) => {
                    match recv_result {
                        Ok(block) => {
                            info!("{}: {}",label, format_block(&block));
                            if block.slot > tip {
                                info!("{}: beyond tip ({} > {})", label, block.slot, tip);
                                block_after_tip = block.slot;
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

#[derive(Debug)]
enum CmdNext {
    DeleteSourceAtIndex(usize)
}

#[derive(Debug)]
enum Command {
    RecvAny,
    RecvGreen,
    RecvBlue,
}

// Slot=0 == None
#[derive(Debug)]
struct State {
    highest_green: Slot,
    highest_blue: Slot,
    tip: Slot,
}

impl State {

    pub fn consume_green(&mut self, green: Slot) -> Command {
        assert!(green > self.highest_green);
        self.highest_green = green;

        if green == self.tip + 1 {
            info!("Thank you, green! progressing tip");
            self.tip = green;
        }

        let next_cmd = self.calc_cmd();

        return next_cmd;
    }

    pub fn consume_blue(&mut self, blue: Slot) -> Command {
        assert!(blue > self.highest_blue);
        self.highest_blue = blue;

        if blue == self.tip + 1 {
            info!("Thank you, blue! progressing tip");
            self.tip = blue;
        }

        let next_cmd = self.calc_cmd();

        return next_cmd;
    }

    fn calc_cmd(&self) -> Command {
        let want_next_green = self.highest_green <= self.tip;
        let want_next_blue = self.highest_blue <= self.tip;

        let next_cmd =
            match (want_next_green, want_next_blue) {
                (true, true) => {
                    Command::RecvAny
                }
                (true, false) => {
                    Command::RecvGreen
                }
                (false, true) => {
                    Command::RecvBlue
                }
                (false, false) => {
                    error!("both streams passed beyond tip - no hope!");
                    Command::RecvAny
                }
            };
        next_cmd
    }



    pub fn consume(self, green: Option<Slot>, blue: Option<Slot>) -> (State, Command) {
        let has_green = green.is_some();
        let has_blue = blue.is_some();
        match (has_green, has_blue) {
            (true, false) => {
                let highest_green = self.highest_green;
                let highest_blue = self.highest_blue;
                let tip = self.tip;
                todo!()
            }
            (false, true) => {
                let highest_green = self.highest_green;
                let highest_blue = self.highest_blue;
                let tip = self.tip;
                todo!()
            }
            (true, true) => {
                if self.highest_green < self.tip && self.highest_blue < self.tip {
                    if self.highest_green < self.highest_blue {

                    } else {

                    }
                }
                let highest_green = self.highest_green;
                let highest_blue = self.highest_blue;
                let tip = self.tip;
                todo!()
            }
            (false, false) => {
                // noop
                let highest_green = self.highest_green;
                let highest_blue = self.highest_blue;
                let tip = self.tip;
                todo!()
            }
        }
    }
}



struct BlockStreamDiskPersistence {
    out_basedir: PathBuf,

}


fn start_monkey_broadcast<T: Clone + Send + 'static>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (block_sx_green, monkey_upstream) = tokio::sync::broadcast::channel::<T>(1024);
    let (monkey_downstream, blocks_notifier_green) = tokio::sync::broadcast::channel::<T>(capacity);

    tokio::spawn(async move {
        let mut monkey_upstream = monkey_upstream;
        'recv_loop: for counter in 0.. {
            let value = monkey_upstream.recv().await.unwrap();
            // info!("forwarding: {}", value);
            // failes if there are no receivers

            if counter % 3 == 0 {
                info!("Delay value");
                tokio::time::sleep(Duration::from_millis(700)).await;
            }
            if counter % 5 == 0 {
                info!("Drop value");
                continue 'recv_loop;
            }

            let send_result = monkey_downstream.send(value);
            match send_result {
                Ok(_) => {
                    info!("forwarded");
                }
                Err(_) => panic!("Should never happen")
            }
        }
    });

    (block_sx_green, blocks_notifier_green)

}


// impl BlockStreamDiskPersistence {
//     pub fn new() -> Self {
//         let out_basedir = PathBuf::from("blocks-dump");
//         Self {
//             out_basedir
//         }
//     }
//
//     pub fn dump_block(&self, block: &ProducedBlock) {
//         let out_block = self.out_basedir.join(block.slot.to_string());
//         let out_file = std::fs::File::create(out_block).unwrap();
//         // bincode::serialize(block).unwrap();
//         info!("dumping to file: {:?}", out_file);
//         serde_json::to_writer_pretty(out_file, &Wrapper(block.clone())).unwrap();
//
//     }
//
// }

