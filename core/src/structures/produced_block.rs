use solana_sdk::{
    borsh0_10::try_from_slice_unchecked,
    commitment_config::CommitmentConfig,
    compute_budget::{self, ComputeBudgetInstruction},
    slot_history::Slot,
    transaction::TransactionError,
};
use solana_transaction_status::{
    option_serializer::OptionSerializer, Reward, RewardType, UiConfirmedBlock,
    UiTransactionStatusMeta,
};

use crate::encoding::BinaryEncoding;

#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub signature: String,
    pub err: Option<TransactionError>,
    pub cu_requested: Option<u32>,
    pub prioritization_fees: Option<u64>,
    pub cu_consumed: Option<u64>,
    pub recent_blockhash: String,
    pub message: String,
}

#[derive(Default, Debug, Clone)]
pub struct ProducedBlock {
    pub transactions: Vec<TransactionInfo>,
    pub leader_id: Option<String>,
    pub blockhash: String,
    pub block_height: u64,
    pub slot: Slot,
    pub parent_slot: Slot,
    pub block_time: u64,
    pub commitment_config: CommitmentConfig,
    pub previous_blockhash: String,
    pub rewards: Option<Vec<Reward>>,
}

impl ProducedBlock {
    pub fn from_ui_block(
        block: UiConfirmedBlock,
        slot: Slot,
        commitment_config: CommitmentConfig,
    ) -> Self {
        let block_height = block.block_height.unwrap_or_default();
        let txs = block.transactions.unwrap_or_default();

        let blockhash = block.blockhash;
        let previous_blockhash = block.previous_blockhash;
        let parent_slot = block.parent_slot;
        let rewards = block.rewards.clone();

        let txs = txs
            .into_iter()
            .filter_map(|tx| {
                let Some(UiTransactionStatusMeta {
                    err,
                    compute_units_consumed,
                    ..
                }) = tx.meta
                else {
                    // ignoring transaction
                    log::info!("Tx with no meta");
                    return None;
                };

                let Some(tx) = tx.transaction.decode() else {
                    // ignoring transaction
                    log::info!("Tx could not be decoded");
                    return None;
                };

                let signature = tx.signatures[0].to_string();
                let cu_consumed = match compute_units_consumed {
                    OptionSerializer::Some(cu_consumed) => Some(cu_consumed),
                    _ => None,
                };

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

                let blockhash = tx.message.recent_blockhash().to_string();
                let message = BinaryEncoding::Base64.encode(tx.message.serialize());

                Some(TransactionInfo {
                    signature,
                    err,
                    cu_requested,
                    prioritization_fees,
                    cu_consumed,
                    recent_blockhash: blockhash,
                    message,
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

        let block_time = block.block_time.unwrap_or(0) as u64;

        ProducedBlock {
            transactions: txs,
            block_height,
            leader_id,
            blockhash,
            previous_blockhash,
            parent_slot,
            block_time,
            slot,
            commitment_config,
            rewards,
        }
    }
}
