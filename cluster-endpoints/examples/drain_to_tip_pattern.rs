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
    // RUST_LOG=info,drain_to_tip_pattern=debug
    tracing_subscriber::fmt::init();

    let (tx, rx) = tokio::sync::broadcast::channel::<Message>(1000);
    let (tx_tip, rx_tip) = tokio::sync::watch::channel::<Message>(Message::new(0));

    start_progressor(rx, rx_tip.clone()).await;

    send_stream(tx.clone()).await;


    info!("Blocking main thread for some time to allow the system to operate...");
    sleep(tokio::time::Duration::from_secs(4)).await;

    info!("Shutting down....");
    drop(tx_tip);
    sleep(tokio::time::Duration::from_secs(1)).await;
    info!("Shutdown completed.");
}



async fn start_progressor(blocks_notifier: Receiver<Message>, mut rx_tip: tokio::sync::watch::Receiver<Message>) {
    info!("Started progressor");
    tokio::spawn(async move {
        let mut blocks_notifier = blocks_notifier.resubscribe();
        let mut local_tip = Message::new(3);
        // block after tip offered by this stream
        let mut block_after_tip = Message::new(0);

        'main_loop: loop {
            select! {
                result = rx_tip.changed() => {
                    if result.is_err() {
                        debug!("Tip variable closed");
                        break 'main_loop;
                    }
                    local_tip = rx_tip.borrow_and_update().clone();
                    info!("++> tip changed to {}", local_tip);
                    // slow down in case of loop
                    // sleep(Duration::from_millis(100)).await;
                }
                recv_result = blocks_notifier.recv(), if !(block_after_tip.slot > local_tip.slot) => {
                    match recv_result {
                        Ok(msg) => {
                            info!("=> recv on: {}", msg);
                            if msg.slot > local_tip.slot {
                                info!("==> beyond tip ({} > {})", msg.slot, local_tip);
                                block_after_tip = msg;
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

    for i in 0..10 {
        message_channel.send(Message::new(i)).unwrap();
        sleep(Duration::from_millis(300)).await;
    }

}





