use anyhow::bail;
use solana_lite_rpc_core::{
    traits::block_storage_interface::BlockStorageInterface, types::BlockStream, AnyhowJoinHandle,
};
use std::sync::Arc;

pub struct History {
    pub block_storage: Arc<dyn BlockStorageInterface>,
}

impl History {
    pub fn start_saving_blocks(&self, mut block_notifier: BlockStream) -> AnyhowJoinHandle {
        let block_storage = self.block_storage.clone();
        tokio::spawn(async move {
            while let Ok(block) = block_notifier.recv().await {
                block_storage.save(block).await?;
            }
            bail!("saving of blocks stopped");
        })
    }
}
