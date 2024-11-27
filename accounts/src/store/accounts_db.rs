use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use solana_accounts_db::accounts::Accounts;
use solana_accounts_db::accounts_db::{AccountsDb as SolanaAccountsDb, AccountsDbConfig, AccountShrinkThreshold, CreateAncientStorage};
use solana_accounts_db::accounts_file::StorageAccess;
use solana_accounts_db::accounts_index::{AccountSecondaryIndexes, AccountsIndexConfig, IndexLimitMb, ScanConfig, ScanResult};
use solana_accounts_db::ancestors::Ancestors;
use solana_accounts_db::partitioned_rewards::TestPartitionedEpochRewards;
use solana_rpc_client_api::filter::RpcFilterType;
use solana_sdk::account::{Account, AccountSharedData, ReadableAccount};
use solana_sdk::clock::{BankId, Slot};
use solana_sdk::genesis_config::ClusterType;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction_context::TransactionAccount;
use task::spawn_blocking;
use tokio::sync::RwLock;
use tokio::task;

use Commitment::{Confirmed, Finalized};
use solana_lite_rpc_core::commitment_utils::Commitment;
use solana_lite_rpc_core::commitment_utils::Commitment::Processed;
use solana_lite_rpc_core::structures::account_data::AccountData;

use crate::account_store_interface::{AccountLoadingError, AccountStorageInterface};

// FIXME what are all those configs
pub const BINS: usize = 8192;
pub const FLUSH_THREADS: usize = 1;

pub const ACCOUNTS_INDEX_CONFIG: AccountsIndexConfig = AccountsIndexConfig {
    bins: Some(BINS),
    flush_threads: Some(FLUSH_THREADS),
    drives: None,
    index_limit_mb: IndexLimitMb::Unspecified,
    ages_to_stay_in_cache: None,
    scan_results_limit_bytes: None,
    started_from_validator: false,
};

pub const ACCOUNTS_DB_CONFIG: AccountsDbConfig = AccountsDbConfig {
    index: Some(ACCOUNTS_INDEX_CONFIG),
    base_working_path: None,
    accounts_hash_cache_path: None,
    shrink_paths: None,
    read_cache_limit_bytes: None,
    write_cache_limit_bytes: None,
    ancient_append_vec_offset: None,
    skip_initial_hash_calc: false,
    exhaustively_verify_refcounts: false,
    create_ancient_storage: CreateAncientStorage::Pack,
    test_partitioned_epoch_rewards: TestPartitionedEpochRewards::None,
    test_skip_rewrites_but_include_in_bank_hash: false,
    storage_access: StorageAccess::Mmap,
};

pub struct AccountsDb {
    accounts: Accounts,
    // FIXME probably RwLock or similar
    commitments: RwLock<HashMap<Commitment, Slot>>,
}

impl AccountsDb {
    pub fn new() -> Self {
        let db = SolanaAccountsDb::new_with_config(
            vec![],
            &ClusterType::MainnetBeta,
            AccountSecondaryIndexes::default(),
            AccountShrinkThreshold::default(),
            Some(ACCOUNTS_DB_CONFIG),
            None,
            Arc::default(),
        );

        let accounts = Accounts::new(Arc::new(db));
        Self {
            accounts,
            commitments: RwLock::new(HashMap::new()),
        }
    }

    pub fn new_for_testing() -> Self {
        let db = SolanaAccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(db));
        Self {
            accounts,
            commitments: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl AccountStorageInterface for AccountsDb {
    async fn update_account(&self, account_data: AccountData, _commitment: Commitment) -> bool {
        self.initilize_or_update_account(account_data).await;
        true
    }

    async fn initilize_or_update_account(&self, account_data: AccountData) {
        let shared_data = account_data.account.to_account_shared_data();
        let account_to_store = [(&account_data.pubkey, &shared_data)];
        self.accounts.store_accounts_cached((account_data.updated_slot, account_to_store.as_slice()));
    }

    async fn get_account(&self, account_pk: Pubkey, commitment: Commitment) -> Result<Option<AccountData>, AccountLoadingError> {
        let ancestors = self.get_ancestors_from_commitment(commitment).await;

        let accounts_db = self.accounts.accounts_db.clone();
        Ok(
            spawn_blocking(move || {
                accounts_db
                    .load_with_fixed_root(&ancestors, &account_pk)
                    .map(|(shared_data, slot)| Self::convert_to_account_data(account_pk, slot, shared_data))
            })
                .await
                .map_err(|e| AccountLoadingError::FailedToSpawnTask(format!("Failed to spawn task: {:?}", e)))?
        )
    }

    async fn get_program_accounts(&self, program_pubkey: Pubkey, account_filter: Option<Vec<RpcFilterType>>, commitment: Commitment) -> Option<Vec<AccountData>> {
        let slot = self.get_slot_from_commitment(commitment).await;
        let ancestors = Ancestors::from(vec![slot]);
        let accounts_db = self.accounts.accounts_db.clone();
        let scan_config = ScanConfig::new(true);

        let transaction_accounts: ScanResult<Vec<TransactionAccount>> = spawn_blocking(move || {
            let filter = move |data: &AccountSharedData| {
                if data.owner() != &program_pubkey {
                    return false;
                }
                match &account_filter {
                    Some(filters) => {
                        filters.iter().all(|filter| {
                            match filter {
                                RpcFilterType::DataSize(size) => data.data().len() == *size as usize,
                                RpcFilterType::Memcmp(cmp) => cmp.bytes_match(data.data()),
                                RpcFilterType::TokenAccountState => unimplemented!() // FIXME
                            }
                        })
                    }
                    None => true
                }
            };

            // implementation from solana accounts.rs with slight adjustments - load_by_program_with_filter
            let mut collector = Vec::new();
            accounts_db
                .scan_accounts(
                    &ancestors,
                    BankId::from(slot),
                    |some_account_tuple| {
                        Self::load_while_filtering(&mut collector, some_account_tuple, |account| {
                            filter(account)
                        })
                    },
                    &scan_config,
                )
                .map(|_| collector)
            // TODO The interface should be adjusted and return Rest<Vec<AccountData>> instead
        }).await.unwrap_or(Ok(vec![]));

        Some(
            transaction_accounts.unwrap()
                .into_iter()
                .map(|ta| { Self::convert_to_account_data(ta.0, slot, ta.1) })
                .collect_vec()
        )
    }


    async fn process_slot_data(&self, slot: Slot, commitment: Commitment) -> Vec<AccountData> {
        if commitment == Finalized {
            self.accounts.add_root(slot);
        }

        let commitments = self.commitments.read().await.clone();

        let processed = commitments.get(&Processed).cloned().unwrap_or(Slot::from(0u64));
        let confirmed = commitments.get(&Confirmed).cloned().unwrap_or(Slot::from(0u64));
        let finalized = commitments.get(&Finalized).cloned().unwrap_or(Slot::from(0u64));

        match commitment {
            Processed => {
                if slot > processed {
                    self.commitments.write().await.insert(Processed, slot);
                }
            }
            Confirmed => {
                if slot > processed {
                    self.commitments.write().await.insert(Processed, slot);
                }
                if slot > confirmed {
                    self.commitments.write().await.insert(Confirmed, slot);
                }
            }
            Finalized => {
                if slot > processed {
                    self.commitments.write().await.insert(Processed, slot);
                }

                if slot > confirmed {
                    self.commitments.write().await.insert(Confirmed, slot);
                }

                if slot > finalized {
                    self.commitments.write().await.insert(Finalized, slot);
                }
            }
        }

        // FIXME do we need to return data from here? - why
        // self.commitments.write().await.insert(commitment, slot);
        vec![]
    }
}

impl AccountsDb {
    // borrowed from solana accounts.rs
    fn load_while_filtering<F: Fn(&AccountSharedData) -> bool>(
        collector: &mut Vec<TransactionAccount>,
        some_account_tuple: Option<(&Pubkey, AccountSharedData, Slot)>,
        filter: F,
    ) {
        if let Some(mapped_account_tuple) = some_account_tuple
            .filter(|(_, account, _)| Self::is_loadable(account.lamports()) && filter(account))
            .map(|(pubkey, account, _slot)| (*pubkey, account))
        {
            collector.push(mapped_account_tuple)
        }
    }

    // borrowed from solana accounts.rs
    fn is_loadable(lamports: u64) -> bool {
        // Don't ever load zero lamport accounts into runtime because
        // the existence of zero-lamport accounts are never deterministic!!
        lamports > 0
    }

    async fn get_slot_from_commitment(&self, commitment: Commitment) -> Slot {
        let lock = self.commitments.read().await;
        let result = lock.get(&commitment).unwrap().clone();
        result
    }

    async fn get_ancestors_from_commitment(&self, commitment: Commitment) -> Ancestors {
        let slot = self.get_slot_from_commitment(commitment).await;
        Ancestors::from(vec![slot])
    }

    fn convert_to_account_data(pk: Pubkey, slot: Slot, shared_data: AccountSharedData) -> AccountData {
        AccountData {
            pubkey: pk,
            account: Arc::new(Account {
                lamports: shared_data.lamports(),
                data: Vec::from(shared_data.data()),
                owner: shared_data.owner().clone(),
                executable: shared_data.executable(),
                rent_epoch: shared_data.rent_epoch(),
            }),
            updated_slot: slot,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use solana_sdk::pubkey::Pubkey;

    use solana_lite_rpc_core::commitment_utils::Commitment::Confirmed;

    use crate::account_store_interface::AccountStorageInterface;
    use crate::store::accounts_db::create_account_data;
    use crate::store::AccountsDb;

    #[tokio::test]
    async fn store_new_account() {
        let test_instance = AccountsDb::new_for_testing();

        let program_key = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
        let account_1_key = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

        let account_1_data = create_account_data(1, account_1_key, program_key, &[0u8; 23]);
        test_instance.initilize_or_update_account(account_1_data).await;

        let result = test_instance.get_account(account_1_key, Confirmed).await;
    }
}

pub fn create_account_data(
    updated_slot: Slot,
    pubkey: Pubkey,
    program: Pubkey,
    data: &[u8],
) -> AccountData {
    AccountData {
        pubkey,
        account: Arc::new(Account {
            lamports: 42,
            data: Vec::from(data),
            owner: program,
            executable: false,
            rent_epoch: 0,
        }),
        updated_slot,
    }


    // #[test]
    // fn test() {
    //     let db = AccountsDb::new_single_for_tests();
    //     let accounts = Accounts::new(Arc::new(db));
    //
    //     // let num_slots = 4;
    //     // let num_accounts = 10_000;
    //     // println!("Creating {num_accounts} accounts");
    //
    //     // let pubkeys: Vec<_> = (0..num_slots)
    //     //     .into_iter()
    //     //     .map(|slot| {
    //     //         let mut pubkeys: Vec<Pubkey> = vec![];
    //     //         create_test_accounts(
    //     //             &accounts,
    //     //             &mut pubkeys,
    //     //             num_accounts / num_slots,
    //     //             slot as u64,
    //     //         );
    //     //         pubkeys
    //     //     })
    //     //     .collect();
    //     //
    //     // let pubkeys: Vec<_> = pubkeys.into_iter().flatten().collect();
    //
    //     // println!("{:?}", pubkeys);
    //     let pubkey = solana_sdk::pubkey::new_rand();
    //     let mut rng = rand::thread_rng();
    //     let program = Pubkey::new_unique();
    //     // let acc = create_random_account(
    //     //     &mut rng,
    //     //     1,
    //     //     pubkey,
    //     //     program,
    //     // );
    //     // println!("{acc:?}");
    //
    //     let account = AccountSharedData::new(
    //         1 as u64,
    //         0,
    //         AccountSharedData::default().owner(),
    //     );
    //
    //     println!("{program:?}");
    //
    //     let account_for_storage = [(&pubkey, &account)];
    //     let to_store = (1u64, account_for_storage.as_slice());
    //
    //     accounts.store_accounts_cached(to_store)
    // }
    //
    // fn create_random_account(
    //     rng: &mut ThreadRng,
    //     updated_slot: Slot,
    //     pubkey: Pubkey,
    //     program: Pubkey,
    // ) -> AccountData {
    //     let length: usize = rng.gen_range(100..1000);
    //     AccountData {
    //         pubkey,
    //         account: Arc::new(Account {
    //             lamports: rng.gen(),
    //             data: (0..length).map(|_| rng.gen::<u8>()).collect_vec(),
    //             owner: program,
    //             executable: false,
    //             rent_epoch: 0,
    //         }),
    //         updated_slot,
    //     }
    // }
}
//
// pub fn create_test_accounts(
//     accounts: &Accounts,
//     pubkeys: &mut Vec<Pubkey>,
//     num: usize,
//     slot: Slot,
// ) {
//     let data_size = 0;
//
//     for t in 0..num {
//         let pubkey = solana_sdk::pubkey::new_rand();
//         let account = AccountSharedData::new(
//             (t + 1) as u64,
//             data_size,
//             AccountSharedData::default().owner(),
//         );
//         // accounts.store_slow_uncached(slot, &pubkey, &account);
//         let random_account = self
//
//         accounts.store_accounts_cached()
//         pubkeys.push(pubkey);
//     }
// }
