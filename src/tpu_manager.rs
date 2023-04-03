use crate::workers::tpu_utils::tpu_service::TpuService;
use std::sync::Arc;

use prometheus::{opts, register_int_counter, IntCounter};

lazy_static::lazy_static! {
static ref TPU_CONNECTION_RESET: IntCounter =
    register_int_counter!(opts!("literpc_tpu_connection_reset", "Number of times tpu connection was reseted")).unwrap();
}

#[derive(Clone)]
pub struct TpuManager {
    pub tpu_service: Arc<TpuService>,
}

impl TpuManager {
    pub fn new(tpu_service: Arc<TpuService>) -> Self {
        Self { tpu_service }
    }

    pub fn estimated_current_slot(&self) -> u64 {
        self.tpu_service.get_estimated_slot()
    }

    pub fn send_transaction(&self, transaction: Vec<u8>) -> anyhow::Result<()> {
        self.tpu_service.send_transaction(transaction)
    }
}
