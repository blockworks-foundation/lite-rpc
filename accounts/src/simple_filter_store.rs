use async_trait::async_trait;
use itertools::Itertools;
use solana_lite_rpc_core::structures::{
    account_data::AccountData,
    account_filter::{AccountFilter, AccountFilterType, AccountFilters, MemcmpFilter},
};
use solana_sdk::pubkey::Pubkey;
use std::collections::{HashMap, HashSet};

use crate::account_filters_interface::AccountFiltersStoreInterface;

#[derive(Clone, PartialEq, Eq)]
enum ProgramIdFilters {
    AllowAll,
    // here first vec will use OR operator and the second will use AND operator
    // i.e vec![vec![A,B]] will be A and B
    // vec![vec![A], vec![B]] will be A or B
    // filters are sorted before storing so that comparing is easier
    ByFilterType(Vec<Vec<AccountFilterType>>),
}

#[derive(Default)]
pub struct SimpleFilterStore {
    accounts: HashSet<Pubkey>,
    program_id_filters: HashMap<Pubkey, ProgramIdFilters>,
}

impl SimpleFilterStore {
    pub fn add_account_filters(&mut self, account_filters: &AccountFilters) {
        for filter in account_filters {
            self.add_account_filter(filter);
        }
    }

    pub fn add_account_filter(&mut self, account_filter: &AccountFilter) {
        for account in &account_filter.accounts {
            self.accounts.insert(*account);
        }

        if let Some(program_id) = &account_filter.program_id {
            match self.program_id_filters.get_mut(program_id) {
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
                        *program_id,
                        account_filter
                            .filters
                            .clone()
                            .map_or(ProgramIdFilters::AllowAll, |filters| {
                                // always save filters in bytes
                                let mut filters = filters.iter().map(|x| match &x {
                                    AccountFilterType::Datasize(_) => x.clone(),
                                    AccountFilterType::Memcmp(memcmp) => {
                                        match memcmp.data {
                                            solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(_) => x.clone(),
                                            solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Base58(_) |
                                            solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Base64(_) => {
                                                AccountFilterType::Memcmp(MemcmpFilter {
                                                    offset: memcmp.offset,
                                                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(memcmp.bytes())
                                                })
                                            },
                                        }
                                    },
                                }).collect_vec();
                                filters.sort();
                                ProgramIdFilters::ByFilterType(vec![filters])
                            }),
                    );
                }
            }
        }
    }

    pub fn contains_account(&self, account_pk: Pubkey) -> bool {
        self.accounts.contains(&account_pk)
    }

    pub fn contains_filter(&self, account_filter: &AccountFilter) -> bool {
        let accounts_match = account_filter
            .accounts
            .iter()
            .all(|pk| self.accounts.contains(pk));

        let program_filter_match = if let Some(program_id) = &account_filter.program_id {
            match self.program_id_filters.get(program_id) {
                Some(subscribed_filters) => {
                    match subscribed_filters {
                        ProgramIdFilters::AllowAll => true,
                        ProgramIdFilters::ByFilterType(filter_list) => {
                            if let Some(filter_to_match) = &account_filter.filters {
                                let mut filter_to_match = filter_to_match.clone();
                                filter_to_match.sort();
                                // matches sorted filter list
                                filter_list.contains(&filter_to_match)
                            } else {
                                false
                            }
                        }
                    }
                }
                None => false,
            }
        } else {
            true
        };

        accounts_match && program_filter_match
    }

    pub fn satisfies_filter(&self, account: &AccountData) -> bool {
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

#[async_trait]
impl AccountFiltersStoreInterface for SimpleFilterStore {
    async fn satisfies(&self, account_data: &AccountData) -> bool {
        self.satisfies_filter(account_data)
    }
}
