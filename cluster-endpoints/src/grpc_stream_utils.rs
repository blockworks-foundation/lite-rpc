use futures::{Stream, StreamExt};
use log::{debug, info, trace, warn};
use std::pin::pin;
use tokio::spawn;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::broadcast::Receiver;
use tokio::task::{AbortHandle};


const BROADCAST_CHANNEL_WARNING_THRESHOLD: usize = 10;

/// note: backpressure will NOT get propagated to upstream but pushed down into broadcast channel
/// service will shut down if upstream gets closed
/// service will NOT shut down if downstream has no receivers
///
/// use `debug_label` to identify the plugger in
/// note: Clone is required
pub fn spawn_plugger_mpcs_to_broadcast_channels<T: Send + Clone + 'static>(
    mut upstream: tokio::sync::mpsc::Receiver<T>,
    downstreams: Vec<tokio::sync::broadcast::Sender<T>>,
    debug_label: &str,
) {
    let debug_label = debug_label.to_string();
    assert!(downstreams.len() > 0, "at least one downstream must be provided");

    // abort plugger task by closing the sender
    let _jh_task = spawn(async move {
        'main_loop: loop {
            match upstream.recv().await {
                Some(msg) => {
                    for (idx, downstream) in downstreams.iter().enumerate() {
                        match downstream.send(msg.clone()) {
                            Ok(receivers) => {
                                trace!("sent data to {} receivers for downstream-{idx} ({debug_label})", receivers);
                            }
                            Err(send_error) => match send_error {
                                SendError(_msg) => {
                                    debug!("no active receivers for downstream-{idx} on channel {debug_label} - skipping message");
                                    continue 'main_loop;
                                }
                            },
                        };
                        if downstream.len() < BROADCAST_CHANNEL_WARNING_THRESHOLD {
                            debug!("messages in downstream-{idx} channel {debug_label}: {}", downstream.len());
                        } else {
                            warn!("messages in downstream-{idx} channel {debug_label}: {}", downstream.len());
                        }
                    }
                }
                None => {
                    info!("plugger {debug_label} source mpsc was closed - aborting plugger task");
                    return; // abort task
                }
            }

        }
    });
}

// note: sending to one broadcast does not require Clone
pub fn spawn_plugger_mpcs_to_broadcast_channel<T: Send + 'static>(
    mut upstream: tokio::sync::mpsc::Receiver<T>,
    downstream: tokio::sync::broadcast::Sender<T>,
    debug_label: &str,
) {
    let debug_label = debug_label.to_string();

    // abort plugger task by closing the sender
    let _jh_task = spawn(async move {
        'main_loop: loop {
            match upstream.recv().await {
                Some(msg) => {
                    match downstream.send(msg) {
                        Ok(receivers) => {
                            trace!("sent data to {} receivers for downstream ({debug_label})", receivers);
                        }
                        Err(send_error) => match send_error {
                            SendError(_msg) => {
                                debug!("no active receivers for downstream on channel {debug_label} - skipping message");
                                continue 'main_loop;
                            }
                        },
                    };
                    if downstream.len() < BROADCAST_CHANNEL_WARNING_THRESHOLD {
                        debug!("messages in downstream channel {debug_label}: {}", downstream.len());
                    } else {
                        warn!("messages in downstream channel {debug_label}: {}", downstream.len());
                    }
                }
                None => {
                    info!("plugger {debug_label} source mpsc was closed - aborting plugger task");
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
