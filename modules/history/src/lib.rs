use futures::stream::Stream;
use futures::StreamExt;
use solana_transaction_status::EncodedConfirmedBlock;
use tokio::pin;
use tokio::task::JoinHandle;

pub async fn start_module(
    full_block_steam: impl Stream<Item = EncodedConfirmedBlock> + std::marker::Send + 'static,
) -> JoinHandle<std::io::Result<()>> {
    let handle = tokio::spawn(async move {
        pin!(full_block_steam);
        while let Some(block) = full_block_steam.next().await {
            //process block
        }

        Ok(())
    });
    handle
}
