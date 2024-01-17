use std::sync::Arc;

use solana_lite_rpc_blocks_processing::block_storage_interface::BlockStorageInterface;

pub struct History {
    pub block_storage: Arc<dyn BlockStorageInterface>,
}
