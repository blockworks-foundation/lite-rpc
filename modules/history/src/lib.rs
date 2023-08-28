use futures::stream::Stream;
use futures::StreamExt;
use solana_sdk::epoch_info::EpochInfo;
use solana_transaction_status::EncodedConfirmedBlock;
use tokio::pin;
use tokio::task::JoinHandle;

pub async fn start_module(
    full_block_stream: impl Stream<Item = EncodedConfirmedBlock> + std::marker::Send + 'static,
    epoch_steam: impl Stream<Item = EpochInfo> + std::marker::Send + 'static,
) -> JoinHandle<std::io::Result<()>> {
    let handle = tokio::spawn(async move {
        pin!(full_block_stream);
        pin!(epoch_steam);

        loop {
            tokio::select! {
                Some(full_block) = full_block_stream.next() => {
                    //process full block
                }
                Some(epoch) = epoch_steam.next() => {
                    //Process epoch
                }
            }
        }
    });
    handle
}
