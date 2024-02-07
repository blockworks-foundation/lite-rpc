use dashmap::DashMap;
use solana_sdk::{account::Account, commitment_config::CommitmentLevel, pubkey::Pubkey};

#[derive(Clone)]
pub struct AccountData {
    pub account_data: Account,
    pub updated_slot: u64,
}

#[derive(Clone)]
pub struct AccountDataByCommitment {
    pub processed_account: Option<AccountData>,
    pub confirmed_account: Option<AccountData>,
    pub finalized_account: Option<AccountData>,
}

pub struct AccountStore {
    account_store: DashMap<Pubkey, AccountDataByCommitment>,
}

impl AccountStore {
    pub fn get_account(&self, account_pk: Pubkey, commitment: CommitmentLevel) -> Option<Account> {
        if let Some(account_by_commitment) = self.account_store.get(&account_pk) {
            match commitment {
                CommitmentLevel::Processed => account_by_commitment
                    .processed_account
                    .as_ref()
                    .map(|account| account.account_data.clone()),
                CommitmentLevel::Confirmed => account_by_commitment
                    .confirmed_account
                    .as_ref()
                    .map(|account| account.account_data.clone()),
                CommitmentLevel::Finalized => account_by_commitment
                    .finalized_account
                    .as_ref()
                    .map(|account| account.account_data.clone()),
                _ => {
                    log::error!("Unknown commitment level while fetching account");
                    None
                }
            }
        } else {
            None
        }
    }
}
