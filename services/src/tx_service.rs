use std::marker::PhantomData;

use solana_lite_rpc_core::tx_store::TxStore;

/// maintains a tx store to confirm txs and notify subscribers

#[derive(Debug, Clone, Default)]
pub struct TxService<Provider> {
    store: TxStore,
    provider: PhantomData<Provider>,
}

impl TxService {
    pub fn new() -> Self {
        Self {
            store: TxStore::new(),
            provider: PhantomData,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        Ok(())
    }
}
