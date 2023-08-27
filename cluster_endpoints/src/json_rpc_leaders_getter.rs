use anyhow::{bail, Context};
use async_trait::async_trait;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_lite_rpc_core::traits::slot_leaders_getter::SlotLeadersGetter;
use std::sync::Arc;

pub struct JsonRpcLeaderGetter {
    pub rpc_client: Arc<RpcClient>,
}

#[async_trait]
impl SlotLeadersGetter for JsonRpcLeaderGetter {
    async fn get_slot_leaders(
        &self,
        from: solana_sdk::slot_history::Slot,
        to: solana_sdk::slot_history::Slot,
    ) -> anyhow::Result<Vec<solana_sdk::pubkey::Pubkey>> {
        if to <= from {
            bail!("invalid arguments for get_slot_leaders");
        }
        let ls = self
            .rpc_client
            .get_slot_leaders(from, to - from)
            .await
            .context("Error getting")?;

        Ok(ls)
    }
}
