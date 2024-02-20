use anyhow::bail;
use log::warn;
use solana_lite_rpc_cluster_endpoints::rpc_polling;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};
use std::ops::RangeInclusive;
use std::sync::Arc;

pub struct FaithfulBlockStore {
    faithful_rpc_client: Arc<RpcClient>, // to fetch legacy blocks from faithful
}

impl FaithfulBlockStore {
    pub fn new(faithful_rpc_client: Arc<RpcClient>) -> Self {
        Self {
            faithful_rpc_client,
        }
    }

    pub fn get_slot_range(&self) -> RangeInclusive<Slot> {
        // TODO
        warn!("slot_range not implemented for FaithfulBlockStore");
        RangeInclusive::new(1, 0) // empty
    }

    pub async fn get_block(&self, slot: Slot) -> anyhow::Result<Box<ProducedBlock>> {
        // TODO check what parameters we want
        let faithful_config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::Full),
            rewards: None,
            commitment: None,
            max_supported_transaction_version: None,
        };

        match self
            .faithful_rpc_client
            .get_block_with_config(slot, faithful_config)
            .await
        {
            Ok(block) => Ok(rpc_polling::poll_blocks::from_ui_block(
                block,
                slot,
                CommitmentConfig::finalized(),
            )),
            Err(err) => {
                bail!(format!("Block {} not found in faithful: {}", slot, err));
            }
        }
    }
}
