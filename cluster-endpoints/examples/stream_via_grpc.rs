use std::path::PathBuf;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use log::info;
use serde::Serializer;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::clock::Slot;
use tokio::select;
use yellowstone_grpc_proto::geyser::CommitmentLevel;

use solana_lite_rpc_cluster_endpoints::grpc_subscription::create_block_processing_task;
use solana_lite_rpc_core::AnyhowJoinHandle;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;

pub const GRPC_VERSION: &str = "1.16.1";

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() {
    // info,solana_lite_rpc_cluster_endpoints=debug,stream_via_grpc=trace
    tracing_subscriber::fmt::init();


    let rpc_client = Arc::new(RpcClient::new("http://127.0.0.0:8899".to_string()));
    // mango validator (mainnet)
    let grpc_addr_mainnet_triton = "http://202.8.9.108:10000".to_string();
    // ams81 (mainnet)
    let grpc_addr_mainnet_ams81 = "http://202.8.8.12:10000".to_string();
    // testnet - NOTE: this connection has terrible lags (almost 5 minutes)
    // let grpc_addr = "http://147.28.169.13:10000".to_string();

    let (block_sx_green, blocks_notifier_green) = tokio::sync::broadcast::channel(10);
    let (block_sx_blue, blocks_notifier_blue) = tokio::sync::broadcast::channel(10);

    let grpc_x_token = None;
    let block_confirmed_task_green: AnyhowJoinHandle = create_block_processing_task(
        grpc_addr_mainnet_triton.clone(),
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



    tokio::spawn(async move {
        let mut blocks_notifier_green = blocks_notifier_green.resubscribe();
        let mut blocks_notifier_blue = blocks_notifier_blue.resubscribe();

        let buffer = Vec::<ProducedBlock>::with_capacity(100);

        let mut command = Command::RecvAny;

        let mut state = State {
            highest_green: 0,
            highest_blue: 0,
            tip: 0,
        };

        loop {

            info!("command: {:?}", command);
            let next_command = match command {
                Command::RecvAny => {
                    select! {
                        next = blocks_notifier_green.recv() => {
                            let block = next.expect("block");
                            info!("green first: {}", format_block(&block));
                            let cmd = state.consume_green(block.slot);
                            cmd
                        }
                        next = blocks_notifier_blue.recv() => {
                            let block = next.expect("block");
                            info!("blue first: {}", format_block(&block));
                            let cmd = state.consume_blue(block.slot);
                            cmd
                        }
                    }
                }
                Command::RecvGreen => {
                    select! {
                        next = blocks_notifier_green.recv() => {
                            let block = next.expect("block");
                            info!("only green: {}", format_block(&block));
                             let cmd = state.consume_green(block.slot);
                            cmd
                        }
                    }
                }
                Command::RecvBlue => {
                    select! {
                        next = blocks_notifier_blue.recv() => {
                            let block = next.expect("block");
                            info!("only blue: {}", format_block(&block));
                            let cmd = state.consume_blue(block.slot);
                            cmd
                        }
                    }
                }
            }; // -- match

            command = next_command;


            // while let Ok(block) = block_stream.recv().await {
            //     info!("block: {:?}@{} (-> {})", block.slot, block.commitment_config.commitment, block.parent_slot);
            //     println!("CSV {},{}", block.slot, block.parent_slot);
            //
            //
            // }
        }
    });

    sleep(Duration::from_secs(1000));

}

fn format_block(block: &ProducedBlock) -> String {
    format!("{:?}@{} (-> {})", block.slot, block.commitment_config.commitment, block.parent_slot)
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
                    panic!("both streams passed beyond tip - no hope!");
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

