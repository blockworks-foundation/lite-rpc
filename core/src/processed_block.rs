use solana_sdk::{
    borsh::try_from_slice_unchecked,
    commitment_config::CommitmentConfig,
    compute_budget::{self, ComputeBudgetInstruction},
    slot_history::Slot,
    transaction::VersionedTransaction,
};
use solana_transaction_status::{RewardType, Rewards};

#[derive(Debug)]
pub struct TransactionInfo {
    pub signature: String,
    pub err: Option<String>,
    pub cu_requested: Option<u32>,
    pub prioritization_fees: Option<u64>,
    pub cu_consumed: Option<u64>,
}

#[derive(Default, Debug)]
pub struct ProcessedBlock {
    pub txs: Vec<TransactionInfo>,
    pub leader_id: Option<String>,
    pub blockhash: String,
    pub block_height: u64,
    pub slot: Slot,
    pub parent_slot: Slot,
    pub block_time: i64,
    pub commitment_config: CommitmentConfig,
}

pub enum BlockProcessorError {
    Incomplete,
}

/// Filtered out information to produce `ProcessedBlock`
/// intermediate information from grpc and rpc
pub struct FilteredBlock {
    pub blockhash: String,
    pub block_height: Option<u64>,
    pub slot: Slot,
    pub parent_slot: Slot,
    pub block_time: Option<i64>,
    pub rewards: Option<Rewards>,
    pub transactions: Option<Vec<FilteredTransaction>>,
}

pub struct FilteredTransaction {
    pub transaction: VersionedTransaction,
    pub meta: Option<FilteredTransactionMeta>,
}

pub struct FilteredTransactionMeta {
    pub err: Option<String>,
    pub compute_units_consumed: Option<u64>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
}

impl ProcessedBlock {
    pub fn process(
        block: FilteredBlock,
        commitment_config: CommitmentConfig,
    ) -> Result<ProcessedBlock, BlockProcessorError> {
        let Some(block_height) = block.block_height else {
            return Err(BlockProcessorError::Incomplete);
        };

        let Some(txs) = block.transactions else {
            return Err(BlockProcessorError::Incomplete);
        };

        let blockhash = block.blockhash;
        let parent_slot = block.parent_slot;

        let txs = txs
            .into_iter()
            .filter_map(|tx| {
                let Some(FilteredTransactionMeta {
                    err,
                    compute_units_consumed,
                    ..
                }) = tx.meta
                else {
                    log::info!("Tx with no meta");
                    return None;
                };

                let tx = tx.transaction;

                let signature = tx.signatures[0].to_string();

                let legacy_compute_budget = tx.message.instructions().iter().find_map(|i| {
                    if i.program_id(tx.message.static_account_keys())
                        .eq(&compute_budget::id())
                    {
                        if let Ok(ComputeBudgetInstruction::RequestUnitsDeprecated {
                            units,
                            additional_fee,
                        }) = try_from_slice_unchecked(i.data.as_slice())
                        {
                            return Some((units, additional_fee));
                        }
                    }
                    None
                });

                let mut cu_requested = tx.message.instructions().iter().find_map(|i| {
                    if i.program_id(tx.message.static_account_keys())
                        .eq(&compute_budget::id())
                    {
                        if let Ok(ComputeBudgetInstruction::SetComputeUnitLimit(limit)) =
                            try_from_slice_unchecked(i.data.as_slice())
                        {
                            return Some(limit);
                        }
                    }
                    None
                });

                let mut prioritization_fees = tx.message.instructions().iter().find_map(|i| {
                    if i.program_id(tx.message.static_account_keys())
                        .eq(&compute_budget::id())
                    {
                        if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(price)) =
                            try_from_slice_unchecked(i.data.as_slice())
                        {
                            return Some(price);
                        }
                    }

                    None
                });

                if let Some((units, additional_fee)) = legacy_compute_budget {
                    cu_requested = Some(units);
                    if additional_fee > 0 {
                        prioritization_fees = Some(((units * 1000) / additional_fee).into())
                    }
                };

                Some(TransactionInfo {
                    signature,
                    err,
                    cu_requested,
                    prioritization_fees,
                    cu_consumed: compute_units_consumed,
                })
            })
            .collect();

        let leader_id = if let Some(rewards) = block.rewards {
            rewards
                .iter()
                .find(|reward| Some(RewardType::Fee) == reward.reward_type)
                .map(|leader_reward| leader_reward.pubkey.clone())
        } else {
            None
        };

        let block_time = block.block_time.unwrap_or(0);

        Ok(ProcessedBlock {
            txs,
            block_height,
            leader_id,
            blockhash,
            slot: block.slot,
            parent_slot,
            block_time,
            commitment_config,
        })
    }
}
