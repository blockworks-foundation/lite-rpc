use futures::stream::Stream;
use futures::StreamExt;
use solana_lite_rpc_core::block_store::BlockInformation;
use tokio::pin;
use tokio::task::JoinHandle;

pub async fn start_module(
    block_info_steam: impl Stream<Item = BlockInformation> + std::marker::Send + 'static,
) -> JoinHandle<std::io::Result<()>> {
    let handle = tokio::spawn(async move {
        pin!(block_info_steam);
        while let Some(block_info) = block_info_steam.next().await {
            //process block_info
        }

        Ok(())
    });
    handle
}
