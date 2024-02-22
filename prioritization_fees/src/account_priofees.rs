use std::{
    collections::{BTreeMap, HashMap},
    sync::{atomic::AtomicU64, Arc},
};

use dashmap::DashMap;
use itertools::Itertools;
use prometheus::{core::GenericGauge, opts, register_int_gauge};
use solana_lite_rpc_core::{
    structures::produced_block::ProducedBlock,
    traits::address_lookup_table_interface::AddressLookupTableInterface,
};
use solana_sdk::{pubkey::Pubkey, slot_history::Slot};

use crate::{
    prioritization_fee_data::{BlockPrioData, PrioFeesData},
    rpc_data::{AccountPrioFeesStats, AccountPrioFeesUpdateMessage},
};

lazy_static::lazy_static! {
    static ref WRITE_ONLY_ACCOUNTS_IN_PRIO_CACHE: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_nb_of_write_accounts_in_priofees_cache", "Number of write accounts in priofees cache")).unwrap();

    static ref ACCOUNTS_IN_PRIO_CACHE: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_nb_of_accounts_in_priofees_cache", "Number of accounts in priofees cache")).unwrap();

    static ref NUMBER_OF_PRIO_DATA_POINTS: GenericGauge<prometheus::core::AtomicI64> =
    register_int_gauge!(opts!("literpc_nb_of_accouts_priofees_datapoints", "Number of priofees accounts data points")).unwrap();
}

pub struct AccountPrio {
    pub stats_by_slot: BTreeMap<u64, BlockPrioData>,
}

#[derive(Clone)]
pub struct AccountPrioStore {
    pub account_by_prio_fees_all: Arc<DashMap<Pubkey, AccountPrio>>,
    pub account_by_prio_fees_writeonly: Arc<DashMap<Pubkey, AccountPrio>>,
    pub number_of_slots_to_save: usize,
    pub last_slot: Arc<AtomicU64>,
    pub address_lookup_tables_impl: Option<Arc<dyn AddressLookupTableInterface>>,
}

impl AccountPrioStore {
    pub fn new(
        number_of_slots_to_save: usize,
        address_lookup_tables_impl: Option<Arc<dyn AddressLookupTableInterface>>,
    ) -> Self {
        Self {
            account_by_prio_fees_all: Arc::new(DashMap::new()),
            account_by_prio_fees_writeonly: Arc::new(DashMap::new()),
            number_of_slots_to_save,
            last_slot: Arc::new(AtomicU64::new(0)),
            address_lookup_tables_impl,
        }
    }

    pub async fn update(&self, produced_block: &ProducedBlock) -> AccountPrioFeesUpdateMessage {
        // sort by ascending order of priority
        let transactions = produced_block
            .transactions
            .iter()
            .filter(|x| !x.is_vote)
            .sorted_by(|a, b| a.prioritization_fees.cmp(&b.prioritization_fees))
            .collect_vec();
        // accounts
        let mut accounts_by_prioritization_write: HashMap<Pubkey, Vec<PrioFeesData>> =
            HashMap::new();
        let mut accounts_by_prioritization_read_write: HashMap<Pubkey, Vec<PrioFeesData>> =
            HashMap::new();

        if let Some(alt_fetcher) = &self.address_lookup_tables_impl {
            let alt_messages = transactions
                .iter()
                .flat_map(|x| &x.address_lookup_tables)
                .collect_vec();

            log::trace!("Checking to reload {} accounts", alt_messages.len());
            alt_fetcher.reload_if_necessary(&alt_messages).await;
        }

        for transaction in transactions {
            let value = PrioFeesData {
                priority: transaction.prioritization_fees.unwrap_or_default(),
                cu_consumed: transaction.cu_consumed.unwrap_or_default(),
            };
            let mut writable_accounts = transaction.writable_accounts.clone();
            let mut readable_accounts = transaction.readable_accounts.clone();

            if let Some(alt_fetcher) = &self.address_lookup_tables_impl {
                for transaction_lookup_table in &transaction.address_lookup_tables {
                    let (mut alts_w, mut alts_r) = alt_fetcher
                        .resolve_addresses_from_lookup_table(transaction_lookup_table)
                        .await;
                    writable_accounts.append(&mut alts_w);
                    readable_accounts.append(&mut alts_r);
                }
            }

            for write_lock in &writable_accounts {
                match accounts_by_prioritization_write.get_mut(write_lock) {
                    Some(acc_vec) => {
                        acc_vec.push(value);
                    }
                    None => {
                        accounts_by_prioritization_write.insert(*write_lock, vec![value]);
                    }
                }

                match accounts_by_prioritization_read_write.get_mut(write_lock) {
                    Some(acc_vec) => {
                        acc_vec.push(value);
                    }
                    None => {
                        accounts_by_prioritization_read_write.insert(*write_lock, vec![value]);
                    }
                }
            }

            for readlock in &readable_accounts {
                match accounts_by_prioritization_read_write.get_mut(readlock) {
                    Some(acc_vec) => {
                        acc_vec.push(value);
                    }
                    None => {
                        accounts_by_prioritization_read_write.insert(*readlock, vec![value]);
                    }
                }
            }
        }

        let slot = produced_block.slot;
        let convert_to_block_prio_data = |data: &Vec<PrioFeesData>| {
            let tx_count = data.len() as u64;
            let cu_consumed = data.iter().map(|x| x.cu_consumed).sum();
            BlockPrioData {
                transaction_data: data.clone(),
                nb_non_vote_tx: tx_count,
                nb_total_tx: tx_count,
                non_vote_cu_consumed: cu_consumed,
                total_cu_consumed: cu_consumed,
            }
        };

        let accounts_by_prioritization_write: HashMap<Pubkey, BlockPrioData> =
            accounts_by_prioritization_write
                .iter()
                .map(|(key, data)| (*key, convert_to_block_prio_data(data)))
                .collect();

        let accounts_by_prioritization_read_write: HashMap<Pubkey, BlockPrioData> =
            accounts_by_prioritization_read_write
                .iter()
                .map(|(key, data)| (*key, convert_to_block_prio_data(data)))
                .collect();

        for (account, data) in &accounts_by_prioritization_write {
            match self.account_by_prio_fees_writeonly.get_mut(account) {
                Some(mut prio) => {
                    prio.stats_by_slot.insert(slot, data.clone());
                }
                None => {
                    let mut prio_fee = AccountPrio {
                        stats_by_slot: BTreeMap::new(),
                    };
                    prio_fee.stats_by_slot.insert(slot, data.clone());
                    self.account_by_prio_fees_writeonly
                        .insert(*account, prio_fee);
                }
            }
        }

        for (account, data) in &accounts_by_prioritization_read_write {
            match self.account_by_prio_fees_writeonly.get_mut(account) {
                Some(mut prio) => {
                    prio.stats_by_slot.insert(slot, data.clone());
                }
                None => {
                    let mut prio_fee = AccountPrio {
                        stats_by_slot: BTreeMap::new(),
                    };
                    prio_fee.stats_by_slot.insert(slot, data.clone());
                    self.account_by_prio_fees_writeonly
                        .insert(*account, prio_fee);
                }
            }
        }

        // cleanup old data
        let min_slot_to_retain = produced_block
            .slot
            .saturating_sub(self.number_of_slots_to_save as u64);
        let cleanup_functor = |iter: &mut AccountPrio| {
            while let Some((k, _)) = iter.stats_by_slot.first_key_value() {
                if *k > min_slot_to_retain {
                    break;
                }
                iter.stats_by_slot.pop_first();
            }
        };
        self.account_by_prio_fees_all.retain(|_, ap| {
            cleanup_functor(ap);
            !ap.stats_by_slot.is_empty()
        });

        self.account_by_prio_fees_writeonly.retain(|_, ap| {
            cleanup_functor(ap);
            !ap.stats_by_slot.is_empty()
        });

        ACCOUNTS_IN_PRIO_CACHE.set(self.account_by_prio_fees_all.len() as i64);
        WRITE_ONLY_ACCOUNTS_IN_PRIO_CACHE.set(self.account_by_prio_fees_writeonly.len() as i64);
        let dp_all: usize = self
            .account_by_prio_fees_all
            .iter()
            .map(|v| v.stats_by_slot.len())
            .sum();
        let dp_writeonly: usize = self
            .account_by_prio_fees_writeonly
            .iter()
            .map(|v| v.stats_by_slot.len())
            .sum();
        NUMBER_OF_PRIO_DATA_POINTS.set((dp_all + dp_writeonly) as i64);

        self.last_slot
            .store(slot, std::sync::atomic::Ordering::Relaxed);

        let account_data: HashMap<Pubkey, AccountPrioFeesStats> =
            accounts_by_prioritization_read_write
                .iter()
                .map(|(account, block_priofee)| {
                    (
                        *account,
                        AccountPrioFeesStats {
                            write_stats: accounts_by_prioritization_write
                                .get(account)
                                .map(|write_data| write_data.calculate_stats())
                                .unwrap_or_default(),
                            all_stats: block_priofee.calculate_stats(),
                        },
                    )
                })
                .collect();
        AccountPrioFeesUpdateMessage {
            slot: produced_block.slot,
            accounts_data: Arc::new(account_data),
        }
    }

    pub fn get_latest_stats(&self, account: &Pubkey) -> (Slot, AccountPrioFeesStats) {
        let all = self
            .account_by_prio_fees_all
            .get(account)
            .map(|x| {
                x.stats_by_slot
                    .last_key_value()
                    .map(|(_, val)| val.clone())
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        let write_only = self
            .account_by_prio_fees_writeonly
            .get(account)
            .map(|x| {
                x.stats_by_slot
                    .last_key_value()
                    .map(|(_, val)| val.clone())
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        (
            self.last_slot.load(std::sync::atomic::Ordering::Relaxed),
            AccountPrioFeesStats {
                write_stats: write_only.calculate_stats(),
                all_stats: all.calculate_stats(),
            },
        )
    }

    pub fn get_n_last_stats(&self, account: &Pubkey, nb: usize) -> (Slot, AccountPrioFeesStats) {
        let functor = |account_prio: &AccountPrio| {
            account_prio
                .stats_by_slot
                .iter()
                .rev()
                .take(nb)
                .fold(BlockPrioData::default(), |agg, (_, rhs)| agg.add(rhs))
        };
        let all = self
            .account_by_prio_fees_all
            .get(account)
            .map(|x| functor(x.value()))
            .unwrap_or_default();
        let write_only = self
            .account_by_prio_fees_writeonly
            .get(account)
            .map(|x| functor(x.value()))
            .unwrap_or_default();
        (
            self.last_slot.load(std::sync::atomic::Ordering::Relaxed),
            AccountPrioFeesStats {
                write_stats: write_only.calculate_stats(),
                all_stats: all.calculate_stats(),
            },
        )
    }
}
