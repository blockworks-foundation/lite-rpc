use std::time::Duration;

use anyhow::Context;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::config::RpcBlockConfig;
use solana_sdk::{commitment_config::CommitmentConfig, slot_history::Slot};
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    processed_block::{
        BlockProcessorError, FilteredBlock, FilteredTransaction, FilteredTransactionMeta,
        ProcessedBlock,
    },
    slot_clock::AVERAGE_SLOT_CHANGE_TIME_IN_MILLIS,
};

pub struct JsonRpcClient;

impl JsonRpcClient {
    pub async fn process(
        rpc_client: &RpcClient,
        slot: Slot,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<Result<ProcessedBlock, BlockProcessorError>> {
        let block = rpc_client
            .get_block_with_config(
                slot,
                RpcBlockConfig {
                    transaction_details: Some(TransactionDetails::Full),
                    commitment: Some(commitment_config),
                    max_supported_transaction_version: Some(0),
                    encoding: Some(UiTransactionEncoding::Base64),
                    rewards: Some(true),
                },
            )
            .await
            .context("failed to get block")?;

        let transactions = block.transactions.map(|txs| {
            txs.into_iter()
                .filter_map(|tx| {
                    let Some(transaction) = tx.transaction.decode() else {
                        log::info!("Tx could not be decoded");
                        return None;
                    };

                    Some(FilteredTransaction {
                        transaction,
                        meta: tx.meta.map(|meta| FilteredTransactionMeta {
                            err: meta.err.map(|err| err.to_string()),
                            compute_units_consumed: meta.compute_units_consumed.into(),
                            fee: meta.fee,
                            pre_balances: meta.pre_balances,
                            post_balances: meta.post_balances,
                        }),
                    })
                })
                .collect()
        });

        Ok(ProcessedBlock::process(
            FilteredBlock {
                blockhash: block.blockhash,
                block_height: block.block_height,
                block_time: block.block_time,
                slot,
                parent_slot: block.parent_slot,
                rewards: block.rewards,
                transactions,
            },
            commitment_config,
        ))
    }

    pub async fn poll_slots(
        rpc_client: &RpcClient,
        slot_tx: UnboundedSender<Slot>,
        commitment_config: CommitmentConfig,
    ) -> anyhow::Result<()> {
        let mut poll_frequency = tokio::time::interval(Duration::from_millis(
            AVERAGE_SLOT_CHANGE_TIME_IN_MILLIS - 100,
        ));

        let mut last_slot = 0;

        loop {
            let slot = rpc_client
                .get_slot_with_commitment(commitment_config)
                .await
                .context("Error getting slot")?;

            // send if slot is greater than last slot
            if slot > last_slot {
                slot_tx.send(slot).context("Error sending slot")?;
            }

            // overwrite last slot
            last_slot = slot;

            // wait for next poll i.e at least 50ms
            poll_frequency.tick().await;
        }
    }
}
