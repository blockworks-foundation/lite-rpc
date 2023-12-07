/// Experiments with the drain-to-tip pattern used for GRPC multiplexing.
///
use std::fmt::Display;
use derive_more::Display;
use futures::StreamExt;

use log::{debug, error, info, warn};
use serde::Serializer;
use tokio::select;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::time::{Duration, sleep};

#[derive(Debug, Clone, Display)]
struct Message {
    slot: u64,
}

impl Message {
    fn new(slot: u64) -> Self {
        Message { slot }
    }
}

#[tokio::main]
async fn main() {
    // RUST_LOG=info,stream_via_grpc=debug,drain_to_tip_pattern=debug
    tracing_subscriber::fmt::init();

    let (tx, rx) = tokio::sync::broadcast::channel::<Message>(1000);
    let (tx_tip, _) = tokio::sync::watch::channel::<Message>(Message::new(0));

    start_progressor(rx, tx_tip.subscribe()).await;

    send_stream(tx.clone()).await;

    // move tip; current tip is 3; next offered slot is 4
    info!("==> force tip to 6 - expect progressor to unblock and offer 7");
    tx_tip.send(Message::new(6)).unwrap();

    info!("Blocking main thread for some time to allow the system to operate...");
    sleep(tokio::time::Duration::from_secs(4)).await;
    info!("Num broadcast subscribers: {}", tx_tip.receiver_count());

    info!("Shutting down....");
    drop(tx_tip);
    sleep(Duration::from_secs(1)).await;
    info!("Shutdown completed.");
}


// this service is dedicated to one source channel which produces a monotonic stream of messages qualified by slot number
// service maintains a tip variable which is updated by different part of system
// service response to tip changes by blocking until the message received from stream has slot number greater than the tip
// service "offers" this message to the rest of the system
async fn start_progressor(mut blocks_notifier: Receiver<Message>, mut rx_tip: tokio::sync::watch::Receiver<Message>) {
    info!("Started progressor");
    tokio::spawn(async move {
        let mut local_tip = Message::new(3);
        // block after tip offered by this stream
        // TODO: block_after_tip is only valid/useful if greater than tip
        let mut highest_block = Message::new(0);

        'main_loop: loop {
            select! {
                result = rx_tip.changed() => {
                    if result.is_err() {
                        debug!("Tip variable closed");
                        break 'main_loop;
                    }
                    local_tip = rx_tip.borrow_and_update().clone();
                    info!("++> tip changed to {}", local_tip);
                    if local_tip.slot <= highest_block.slot {
                        info!("!! next offered slot is invalid: {} <= {}", highest_block.slot, local_tip);
                    }
                    // slow down in case of loop
                    // sleep(Duration::from_millis(100)).await;
                }

                // here goes the strategy: either we get a new block OR a timeout
                recv_result = blocks_notifier.recv(), if !(highest_block.slot > local_tip.slot) => {
                    debug!("!! block_after_tip.slot > local_tip.slot: {} > {}", highest_block.slot, local_tip.slot);
                    match recv_result {
                        Ok(msg) => {
                            info!("=> recv: {}", msg);
                            if msg.slot > local_tip.slot {
                                info!("==> offer next slot ({} -> {})", local_tip, msg.slot);
                                highest_block = msg;
                                // offer_block_sender.send(OfferBlockMsg::NextSlot(label.clone(), block_after_tip.clone())).await.unwrap();
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
        } // -- main loop

        info!("Shutting down progressor.");
    });
}


async fn send_stream(message_channel: Sender<Message>) {

    // tip is 3

    // drain 0 to 3; offer 4, then block
    for i in 0..10 {
        info!("> sending {} (queue size={})", i, message_channel.len());
        message_channel.send(Message::new(i)).unwrap();
        sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(message_channel.len(), 5);

}





