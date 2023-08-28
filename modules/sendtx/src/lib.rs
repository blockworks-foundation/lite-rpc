use futures::stream::Stream;
use futures::StreamExt;
use solana_lite_rpc_core::block_store::BlockInformation;
use tokio::pin;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

pub async fn start_module(
    block_info_steam: impl Stream<Item = BlockInformation> + std::marker::Send + 'static,
    slot_steam: impl Stream<Item = Result<u64, BroadcastStreamRecvError>> + std::marker::Send + 'static,
) -> JoinHandle<std::io::Result<()>> {
    let handle = tokio::spawn(async move {
        pin!(slot_steam);
        pin!(block_info_steam);

        loop {
            tokio::select! {
                Some(block_info) = block_info_steam.next() => {
                    //process block_info
                }
                Some(slot) = slot_steam.next() => {
                    //Process slot
                }
            }
        }
    });
    handle
}
