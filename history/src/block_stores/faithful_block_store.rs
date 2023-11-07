use std::sync::Arc;
use anyhow::bail;
use log::debug;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::clock::Slot;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};
use solana_lite_rpc_core::commitment_utils::Commitment;
use solana_lite_rpc_core::structures::produced_block::ProducedBlock;


pub struct FaithfulBlockStore {
    faithful_rpc_client: Arc<RpcClient>, // to fetch legacy blocks from faithful
}

impl FaithfulBlockStore {
    pub fn new(faithful_rpc_client: Arc<RpcClient>) -> Self {
        Self {
            faithful_rpc_client,
        }
    }

    pub async fn get_block(&self, slot: Slot) -> anyhow::Result<ProducedBlock> {

        // TODO check what parameters we want
        let faithful_config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base58),
            transaction_details: Some(TransactionDetails::Full),
            rewards: None,
            commitment: None,
            max_supported_transaction_version: None,
        };

        match self.faithful_rpc_client
            .get_block_with_config(slot, faithful_config)
            .await {
            Ok(block) => {
                return Ok(ProducedBlock::from_ui_block(
                    block,
                    slot,
                    Commitment::Finalized,
                ));
            }
            Err(err) => {
                bail!(format!("Block {} not found in faithful: {}", slot, err));
            }
        }

    }
}
