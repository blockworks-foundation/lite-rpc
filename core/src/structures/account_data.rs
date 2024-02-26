use std::sync::Arc;

use solana_rpc_client_api::filter::RpcFilterType;
use solana_sdk::{account::Account, pubkey::Pubkey, slot_history::Slot};
use tokio::sync::broadcast::Receiver;

use crate::commitment_utils::Commitment;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct AccountData {
    pub pubkey: Pubkey,
    pub account: Arc<Account>,
    pub updated_slot: Slot,
}

impl AccountData {
    pub fn allows(&self, filter: &RpcFilterType) -> bool {
        match filter {
            RpcFilterType::DataSize(size) => self.account.data.len() as u64 == *size,
            RpcFilterType::Memcmp(compare) => compare.bytes_match(&self.account.data),
            RpcFilterType::TokenAccountState => {
                // todo
                false
            }
        }
    }
}

#[derive(Clone)]
pub struct AccountNotificationMessage {
    pub data: AccountData,
    pub commitment: Commitment,
}

pub type AccountStream = Receiver<AccountNotificationMessage>;
