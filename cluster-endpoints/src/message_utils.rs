use solana_sdk::{
    borsh1::try_from_slice_unchecked,
    compute_budget::{self, ComputeBudgetInstruction},
    message::v0::Message,
};

pub fn get_cu_requested_from_message(message: &Message) -> Option<u32> {
    message.instructions.iter().find_map(|i| {
        if i.program_id(&message.account_keys)
            .eq(&compute_budget::id())
        {
            if let Ok(ComputeBudgetInstruction::SetComputeUnitLimit(limit)) =
                try_from_slice_unchecked(i.data.as_slice())
            {
                return Some(limit);
            }
        }
        None
    })
}

pub fn get_prioritization_fees_from_message(message: &Message) -> Option<u64> {
    message.instructions.iter().find_map(|i| {
        if i.program_id(&message.account_keys)
            .eq(&compute_budget::id())
        {
            if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(price)) =
                try_from_slice_unchecked(i.data.as_slice())
            {
                return Some(price);
            }
        }

        None
    })
}
