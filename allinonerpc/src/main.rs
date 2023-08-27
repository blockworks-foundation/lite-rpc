use crate::stream::BlockInfokStreamConnector;
use crate::stream::FullblockStreamConnector;
use crate::stream::SlotStreamConnector;
use tokio_stream::StreamExt;

mod stream;

const GRPC_URL: &str = "http://127.0.0.0:10000";

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
pub async fn main() -> anyhow::Result<()> {
    //slot connector
    let slot_connector = SlotStreamConnector::new(100);
    let slot_notifier = slot_connector.get_sender();
    //convert broadcast to a sink
    let (slot_sender, mut slot_receiver) = futures::channel::mpsc::unbounded::<u64>();
    tokio::spawn(async move {
        while let Some(slot) = slot_receiver.next().await {
            if let Err(err) = slot_notifier.send(slot) {
                log::error!("Error during slot broadcast from sink:{err}");
            }
        }
    });

    let block_stream = FullblockStreamConnector::new();
    let block_sink = block_stream.get_sender_sink();
    let block_info_stream = BlockInfokStreamConnector::new();
    let block_info_sink = block_info_stream.get_sender_sink();
    let history_handle = history::start_module(block_stream.subscribe()).await;
    let send_tx_handle = sendtx::start_module(block_info_stream.subscribe()).await;
    let consensus_handle = consensus::start_module(
        GRPC_URL.to_string(),
        slot_sender,
        block_sink,
        block_info_sink,
    )
    .await;

    tokio::select! {
        err = history_handle => {
            // This should never happen
            unreachable!("{err:?}")
        }
        err = consensus_handle => {
            // This should never happen
            unreachable!("{err:?}")
        }
        err = send_tx_handle => {
            // This should never happen
            unreachable!("{err:?}")
        }

    }
}
