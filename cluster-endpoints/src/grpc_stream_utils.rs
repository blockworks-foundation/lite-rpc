use futures::{Stream, StreamExt};
use log::{debug, info, trace};
use std::pin::pin;
use futures::future::select_all;
use geyser_grpc_connector::Message;
use tokio::spawn;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::broadcast::Receiver;
use tokio::task::{AbortHandle, JoinHandle};


/// note: backpressure will NOT get propagated to upstream but pushed down into broadcast channel
pub fn spawn_plugger_mpcs_to_broadcast<T: Send + 'static>(
    mut upstream: tokio::sync::mpsc::Receiver<T>,
    downstream: tokio::sync::broadcast::Sender<T>,
    // TODO allow multiple downstreams + fanout
) {
    let jh_channelizer = spawn(async move {
        'main_loop: loop {
            match upstream.recv().await {
                Some(msg) => {
                    match downstream.send(msg) {
                        Ok(receivers) => {
                            trace!("sent data to {} receivers", receivers);
                        }
                        Err(send_error) => match send_error {
                            SendError(_msg) => {
                                debug!("no active receivers - skipping message");
                                continue 'main_loop;
                            }
                        },
                    };
                    debug!("messages in broadcast channel: {}", downstream.len());
                }
                None => {
                    info!("channelizer source stream was closed - aborting channelizer task");
                    return; // abort task
                }
            }

        }
    });
}


pub fn channelize_stream<T>(
    grpc_source_stream: impl Stream<Item = T> + Send + 'static,
    broadcast_channel_capacity: usize,
) -> (Receiver<T>, AbortHandle)
where
    T: Clone + Send + 'static,
{
    let (sender_tx, output_rx) = tokio::sync::broadcast::channel::<T>(broadcast_channel_capacity);

    let jh_channelizer = spawn(async move {
        let mut source_stream = pin!(grpc_source_stream);
        'main_loop: loop {
            match source_stream.next().await {
                Some(msg) => {
                    match sender_tx.send(msg) {
                        Ok(receivers) => {
                            trace!("sent data to {} receivers", receivers);
                        }
                        Err(send_error) => match send_error {
                            SendError(_msg) => {
                                debug!("no active receivers - skipping message");
                                continue 'main_loop;
                            }
                        },
                    };
                    debug!("messages in broadcast channel: {}", sender_tx.len());
                }
                None => {
                    info!("channelizer source stream was closed - aborting channelizer task");
                    return; // abort task
                }
            }

        }
    });

    (output_rx, jh_channelizer.abort_handle())
}
