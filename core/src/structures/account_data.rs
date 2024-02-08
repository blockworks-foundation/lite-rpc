use solana_rpc_client_api::filter::RpcFilterType;
use solana_sdk::{account::Account, hash::Hash, pubkey::Pubkey, slot_history::Slot};
use tokio::sync::broadcast::Receiver;

use crate::commitment_utils::Commitment;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct AccountData {
    pub account: Account,
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
    pub account_pk: Pubkey,
    pub commitment: Commitment,
    pub block_hash: Hash,
}

pub type AccountStream = Receiver<AccountNotificationMessage>;
