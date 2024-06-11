use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use solana_lite_rpc_core::structures::{
    account_data::AccountData,
    account_filter::{AccountFilter, AccountFilterType, AccountFilters},
};
use solana_sdk::pubkey::Pubkey;

#[derive(Clone)]
enum ProgramIdFilters {
    AllowAll,
    // here first vec will use OR operator and the second will use AND operator
    // i.e vec![vec![A,B]] will be A and B
    // vec![vec![A], vec![B]] will be A or B
    ByFilterType(Vec<Vec<AccountFilterType>>),
}

#[derive(Default)]
pub struct FilteredAccounts {
    accounts: HashSet<Pubkey>,
    program_id_filters: HashMap<Pubkey, ProgramIdFilters>,
}

impl FilteredAccounts {
    pub fn add_account_filters(&mut self, account_filters: &AccountFilters) {
        for filter in account_filters {
            self.add_account_filter(filter);
        }
    }

    pub fn add_account_filter(&mut self, account_filter: &AccountFilter) {
        for account in &account_filter.accounts {
            let pk = Pubkey::from_str(account).expect("Account filter pubkey should be valid");
            self.accounts.insert(pk);
        }

        if let Some(program_id) = &account_filter.program_id {
            let program_id =
                Pubkey::from_str(program_id).expect("Account filter pubkey should be valid");
            match self.program_id_filters.get_mut(&program_id) {
                Some(program_filters) => {
                    match program_filters {
                        ProgramIdFilters::AllowAll => {
                            // do nothing as all the accounts are already subscribed for the program
                        }
                        ProgramIdFilters::ByFilterType(current_filters) => {
                            if let Some(new_filters) = &account_filter.filters {
                                current_filters.push(new_filters.clone());
                            } else {
                                // the new filters will subscribe to all the filters
                                *program_filters = ProgramIdFilters::AllowAll;
                            }
                        }
                    }
                }
                None => {
                    self.program_id_filters.insert(
                        program_id,
                        account_filter
                            .filters
                            .clone()
                            .map_or(ProgramIdFilters::AllowAll, |filters| {
                                ProgramIdFilters::ByFilterType(vec![filters])
                            }),
                    );
                }
            }
        }
    }

    pub fn satisfies(&self, account: &AccountData) -> bool {
        if self.accounts.contains(&account.pubkey) {
            return true;
        }

        if let Some(program_filters) = self.program_id_filters.get(&account.account.owner) {
            match program_filters {
                ProgramIdFilters::AllowAll => {
                    return true;
                }
                ProgramIdFilters::ByFilterType(filters) => {
                    return filters.iter().any(|filter| {
                        filter
                            .iter()
                            .all(|x| x.allows(&account.account.data.data()))
                    })
                }
            }
        }

        false
    }
}
