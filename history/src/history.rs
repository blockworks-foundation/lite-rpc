use solana_lite_rpc_core::traits::block_storage_interface::BlockStorageInterface;
use std::sync::Arc;

pub struct History {
    pub block_storage: Arc<dyn BlockStorageInterface>,
}
