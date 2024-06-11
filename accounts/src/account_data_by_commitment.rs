use std::collections::BTreeMap;

use solana_lite_rpc_core::{commitment_utils::Commitment, structures::account_data::AccountData};
use solana_sdk::{clock::Slot, pubkey::Pubkey};

#[derive(Default)]
pub struct AccountDataByCommitment {
    pub pubkey: Pubkey,
    // should have maximum 32 entries, all processed slots which are not yet finalized
    pub processed_accounts: BTreeMap<Slot, AccountData>,
    pub confirmed_account: Option<AccountData>,
    pub finalized_account: Option<AccountData>,
}

impl AccountDataByCommitment {
    #[allow(deprecated)]
    pub fn get_account_data(&self, commitment: Commitment) -> Option<AccountData> {
        match commitment {
            Commitment::Processed => self
                .processed_accounts
                .last_key_value()
                .map(|(_, v)| v.clone())
                .or(self.confirmed_account.clone()),
            Commitment::Confirmed => self.confirmed_account.clone(),
            Commitment::Finalized => self.finalized_account.clone(),
        }
    }

    // Should be used when accounts is created by geyser notification
    pub fn new(data: AccountData, commitment: Commitment) -> Self {
        let mut processed_accounts = BTreeMap::new();
        processed_accounts.insert(data.updated_slot, data.clone());
        AccountDataByCommitment {
            pubkey: data.pubkey,
            processed_accounts,
            confirmed_account: if commitment == Commitment::Confirmed
                || commitment == Commitment::Finalized
            {
                Some(data.clone())
            } else {
                None
            },
            finalized_account: if commitment == Commitment::Finalized {
                Some(data)
            } else {
                None
            },
        }
    }

    // should be called with finalized accounts data
    // when accounts storage is being warmed up
    pub fn initialize(data: AccountData) -> Self {
        let mut processed_accounts = BTreeMap::new();
        processed_accounts.insert(data.updated_slot, data.clone());
        AccountDataByCommitment {
            pubkey: data.pubkey,
            processed_accounts,
            confirmed_account: Some(data.clone()),
            finalized_account: Some(data),
        }
    }

    pub fn update(&mut self, data: AccountData, commitment: Commitment) -> bool {
        // if commitmentment is processed check and update processed
        // if commitmentment is confirmed check and update processed and confirmed
        // if commitmentment is finalized check and update all
        let update_confirmed = self
            .confirmed_account
            .as_ref()
            .map(|x| x.updated_slot < data.updated_slot)
            .unwrap_or(true);
        let update_finalized = self
            .finalized_account
            .as_ref()
            .map(|x| x.updated_slot < data.updated_slot)
            .unwrap_or(true);

        let mut updated = false;
        // processed not present for the slot
        // grpc can send multiple inter transaction changed account states for same slot
        // we have to update till we get the last
        if commitment == Commitment::Processed
            || !self.processed_accounts.contains_key(&data.updated_slot)
        {
            self.processed_accounts
                .insert(data.updated_slot, data.clone());
            updated = true;
        }

        match commitment {
            Commitment::Confirmed => {
                if update_confirmed {
                    self.confirmed_account = Some(data);
                    updated = true;
                }
            }
            Commitment::Finalized => {
                if update_confirmed {
                    self.confirmed_account = Some(data.clone());
                    updated = true;
                }
                if update_finalized {
                    self.finalized_account = Some(data);
                    updated = true;
                }
            }
            Commitment::Processed => {
                // processed already treated
            }
        }
        updated
    }
    // this method will promote processed account to confirmed account to finalized account
    // returns promoted account
    pub fn promote_slot_commitment(
        &mut self,
        _pubkey: Pubkey,
        slot: Slot,
        commitment: Commitment,
    ) -> Option<(AccountData, Option<AccountData>)> {
        if let Some(account_data) = self.processed_accounts.get(&slot).cloned() {
            match commitment {
                Commitment::Processed => {
                    // do nothing
                    None
                }
                Commitment::Confirmed => {
                    if self
                        .confirmed_account
                        .as_ref()
                        .map(|acc| acc.updated_slot)
                        .unwrap_or_default()
                        < slot
                    {
                        let prev_data = self.confirmed_account.clone();
                        self.confirmed_account = Some(account_data.clone());
                        Some((account_data, prev_data))
                    } else {
                        None
                    }
                }
                Commitment::Finalized => {
                    // slot finalized remove data from processed
                    while self.processed_accounts.len() > 1
                        && self
                            .processed_accounts
                            .first_key_value()
                            .map(|(s, _)| *s)
                            .unwrap_or(u64::MAX)
                            <= slot
                    {
                        self.processed_accounts.pop_first();
                    }

                    if self
                        .finalized_account
                        .as_ref()
                        .map(|acc| acc.updated_slot)
                        .unwrap_or_default()
                        < slot
                    {
                        let prev_data = self.finalized_account.clone();
                        self.finalized_account = Some(account_data.clone());
                        Some((account_data, prev_data))
                    } else {
                        None
                    }
                }
            }
        } else if commitment == Commitment::Finalized {
            // remove processed slot data
            while self.processed_accounts.len() > 1
                && self
                    .processed_accounts
                    .first_key_value()
                    .map(|(s, _)| *s)
                    .unwrap_or(u64::MAX)
                    <= slot
            {
                self.processed_accounts.pop_first();
            }

            None
        } else {
            //log::warn!("Expected to have processed account update for slot {} data and pk {}", slot, pubkey);
            None
        }
    }
}
