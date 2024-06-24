use async_trait::async_trait;
use solana_lite_rpc_core::structures::{
    account_data::AccountData,
    account_filter::{AccountFilter, AccountFilterType, AccountFilters, MemcmpFilter},
};
use solana_sdk::pubkey::Pubkey;
use std::collections::{BTreeSet, HashMap, HashSet};

use crate::account_filters_interface::AccountFiltersStoreInterface;

#[derive(Clone, PartialEq, Eq)]
enum ProgramIdFilters {
    AllowAll,
    // here first vec will use OR operator and the second will use AND operator
    // i.e vec![vec![A,B]] will be A and B
    // vec![vec![A], vec![B]] will be A or B
    // using btree set because the filters will be stored sorted and comparing them is easier
    ByFilterType(Vec<BTreeSet<AccountFilterType>>),
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
                                current_filters.push(new_filters.iter().cloned().collect());
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
                                let filters = filters.iter().map(|x| match &x {
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
                                }).collect();
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
                                // matches sorted filter list
                                let filter_to_match: BTreeSet<AccountFilterType> =
                                    filter_to_match.iter().cloned().collect();
                                filter_list.iter().any(|stored_filters| {
                                    if filter_to_match.len() >= stored_filters.len() {
                                        let subset = filter_to_match
                                            .iter()
                                            .take(stored_filters.len())
                                            .cloned()
                                            .collect::<BTreeSet<_>>();
                                        subset == *stored_filters
                                    } else {
                                        false
                                    }
                                })
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

#[cfg(test)]
mod tests {
    use solana_lite_rpc_core::structures::account_filter::{
        AccountFilter, AccountFilterType, MemcmpFilter,
    };
    use solana_sdk::pubkey::Pubkey;

    use super::SimpleFilterStore;

    #[test]
    fn test_program_that_allows_all() {
        let mut simple_store = SimpleFilterStore::default();
        let program_id = Pubkey::new_unique();
        simple_store.add_account_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: None,
        });
        assert!(simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: None,
        }));

        assert!(!simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(Pubkey::new_unique()),
            filters: None,
        }));

        assert!(simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![AccountFilterType::Datasize(100)]),
        }));

        // contains a subfilter
        assert!(simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![
                AccountFilterType::Datasize(100),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![123]
                    )
                })
            ]),
        }));
    }

    #[test]
    fn test_program_that_partial_filtering() {
        let mut simple_store = SimpleFilterStore::default();
        let program_id = Pubkey::new_unique();
        simple_store.add_account_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![
                AccountFilterType::Datasize(100),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![123],
                    ),
                }),
            ]),
        });
        assert!(!simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: None,
        }));

        assert!(!simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(Pubkey::new_unique()),
            filters: None,
        }));

        // it cannot detect supersets
        assert!(!simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![AccountFilterType::Datasize(100)]),
        }));

        // contains a subfilter
        assert!(simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![
                AccountFilterType::Datasize(100),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![123]
                    )
                })
            ]),
        }));

        // contains in different order
        assert!(simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![123]
                    )
                }),
                AccountFilterType::Datasize(100)
            ]),
        }));

        // can detect subsets
        assert!(simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 5,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![8, 9, 10]
                    )
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![123]
                    )
                }),
                AccountFilterType::Datasize(100)
            ]),
        }));
    }

    #[test]
    fn test_program_that_partial_filtering_2() {
        let mut simple_store = SimpleFilterStore::default();
        let program_id = Pubkey::new_unique();
        simple_store.add_account_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![
                AccountFilterType::Datasize(100),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![1, 2, 3],
                    ),
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 10,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![5, 6, 7],
                    ),
                }),
            ]),
        });
        assert!(!simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: None,
        }));

        assert!(!simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(Pubkey::new_unique()),
            filters: None,
        }));

        // for now it cannot detect subsets
        assert!(!simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![AccountFilterType::Datasize(100)]),
        }));

        // contains a subfilter
        assert!(!simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![
                AccountFilterType::Datasize(100),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![123]
                    )
                })
            ]),
        }));

        assert!(simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![
                AccountFilterType::Datasize(100),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![1, 2, 3]
                    )
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 10,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![5, 6, 7]
                    )
                })
            ]),
        }));

        assert!(simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![1, 2, 3]
                    )
                }),
                AccountFilterType::Datasize(100),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 10,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![5, 6, 7]
                    )
                })
            ]),
        }));

        assert!(simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![1, 2, 3]
                    )
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 10,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![5, 6, 7]
                    )
                }),
                AccountFilterType::Datasize(100),
            ]),
        }));

        assert!(simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 10,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![5, 6, 7]
                    )
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![1, 2, 3]
                    )
                }),
                AccountFilterType::Datasize(100),
            ]),
        }));

        // can detect subsets
        assert!(simple_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id),
            filters: Some(vec![
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 10,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![5, 6, 7]
                    )
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 2,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![1, 2, 3]
                    )
                }),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 20,
                    data: solana_lite_rpc_core::structures::account_filter::MemcmpFilterData::Bytes(
                        vec![4, 5, 6]
                    )
                }),
                AccountFilterType::Datasize(100),
            ]),
        }));
    }
}
