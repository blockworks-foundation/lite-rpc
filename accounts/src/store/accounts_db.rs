use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use itertools::Itertools;
use solana_accounts_db::accounts::Accounts;
use solana_accounts_db::accounts_db::{AccountsDb as SolanaAccountsDb, AccountsDbConfig, AccountShrinkThreshold, CreateAncientStorage};
use solana_accounts_db::accounts_file::StorageAccess;
use solana_accounts_db::accounts_index::{AccountSecondaryIndexes, AccountsIndexConfig, IndexLimitMb};
use solana_accounts_db::ancestors::Ancestors;
use solana_accounts_db::partitioned_rewards::TestPartitionedEpochRewards;
use solana_rpc_client_api::filter::RpcFilterType;
use solana_sdk::account::{Account, AccountSharedData, ReadableAccount};
use solana_sdk::clock::{BankId, Slot};
use solana_sdk::genesis_config::ClusterType;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction_context::TransactionAccount;

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
    processed_slot: AtomicU64,
    confirmed_slot: AtomicU64,
    finalised_slot: AtomicU64,
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
            processed_slot: AtomicU64::new(0),
            confirmed_slot: AtomicU64::new(0),
            finalised_slot: AtomicU64::new(0),
        }
    }

    pub fn new_for_testing() -> Self {
        let db = SolanaAccountsDb::new_single_for_tests();
        let accounts = Accounts::new(Arc::new(db));
        Self {
            accounts,
            processed_slot: AtomicU64::new(0),
            confirmed_slot: AtomicU64::new(0),
            finalised_slot: AtomicU64::new(0),
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
        let ancestors = self.get_ancestors_from_commitment(commitment);
        Ok(
            self.accounts
                .load_with_fixed_root(&ancestors, &account_pk)
                .map(|(shared_data, slot)| Self::convert_to_account_data(account_pk, slot, shared_data))
        )
    }

    async fn get_program_accounts(&self, program_pubkey: Pubkey, account_filter: Option<Vec<RpcFilterType>>, commitment: Commitment) -> Option<Vec<AccountData>> {
        let slot = self.get_slot_from_commitment(commitment);

        let filter = |data: &AccountSharedData| {
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

        let transaction_accounts: Vec<TransactionAccount> = self.accounts.load_by_program_slot(slot, Some(&program_pubkey))
            .into_iter()
            .filter(|ta| filter(&ta.1))
            .collect();


        let result = transaction_accounts
            .into_iter()
            .map(|ta| { Self::convert_to_account_data(ta.0, slot, ta.1) })
            .collect_vec();

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }


    async fn process_slot_data(&self, slot: Slot, commitment: Commitment) -> Vec<AccountData> {
        if commitment == Finalized {
            self.accounts.add_root(slot);
        }

        let processed = self.processed_slot.load(Ordering::Relaxed);
        let confirmed = self.confirmed_slot.load(Ordering::Relaxed);
        let finalized = self.finalised_slot.load(Ordering::Relaxed);

        match commitment {
            Processed => {
                if slot > processed {
                    self.processed_slot.store(slot, Ordering::Relaxed);
                }
            }
            Confirmed => {
                if slot > processed {
                    self.processed_slot.store(slot, Ordering::Relaxed);
                }
                if slot > confirmed {
                    self.confirmed_slot.store(slot, Ordering::Relaxed);
                }
            }
            Finalized => {
                if slot > processed {
                    self.processed_slot.store(slot, Ordering::Relaxed);
                }

                if slot > confirmed {
                    self.confirmed_slot.store(slot, Ordering::Relaxed);
                }

                if slot > finalized {
                    self.finalised_slot.store(slot, Ordering::Relaxed);
                }
            }
        }

        assert!(self.processed_slot.load(Ordering::Relaxed) >= self.confirmed_slot.load(Ordering::Relaxed));
        assert!(self.confirmed_slot.load(Ordering::Relaxed) >= self.finalised_slot.load(Ordering::Relaxed));

        self.accounts.load_all(&Ancestors::from(vec![slot]), slot, false)
            .unwrap()
            .into_iter()
            .filter(|(_, _, updated_slot)| *updated_slot == slot)
            .map(|(key, data, slot)| Self::convert_to_account_data(key, slot, data))
            .collect_vec()
    }
}

impl AccountsDb {
    fn get_slot_from_commitment(&self, commitment: Commitment) -> Slot {
        match commitment {
            Processed => self.processed_slot.load(Ordering::Relaxed),
            Confirmed => self.confirmed_slot.load(Ordering::Relaxed),
            Finalized => self.finalised_slot.load(Ordering::Relaxed)
        }
    }

    fn get_ancestors_from_commitment(&self, commitment: Commitment) -> Ancestors {
        let slot = self.get_slot_from_commitment(commitment);
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

    use solana_lite_rpc_core::commitment_utils::Commitment::Processed;

    use crate::account_store_interface::AccountStorageInterface;
    use crate::store::accounts_db::create_account_data;
    use crate::store::AccountsDb;

    #[tokio::test]
    async fn store_new_account() {
        let ti = AccountsDb::new_for_testing();

        let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
        let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

        let ad = create_account_data(2, ak, pk, 1);
        ti.initilize_or_update_account(ad).await;

        ti.process_slot_data(2, Processed).await;

        let result = ti.get_account(ak, Processed).await;
        assert!(result.is_ok());
        let data = result.unwrap().unwrap();
        assert_eq!(data.updated_slot, 2);
        assert_eq!(data.account.lamports, 1);
    }

    mod get_account {
        use std::str::FromStr;

        use solana_sdk::pubkey::Pubkey;

        use solana_lite_rpc_core::commitment_utils::Commitment::{Confirmed, Finalized, Processed};

        use crate::account_store_interface::AccountStorageInterface;
        use crate::store::accounts_db::create_account_data;
        use crate::store::AccountsDb;

        #[tokio::test]
        async fn different_commitments() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

            ti.process_slot_data(5, Processed).await;
            ti.process_slot_data(4, Confirmed).await;
            ti.process_slot_data(3, Finalized).await;

            ti.initilize_or_update_account(create_account_data(5, ak, pk, 10)).await;
            ti.initilize_or_update_account(create_account_data(4, ak, pk, 20)).await;
            ti.initilize_or_update_account(create_account_data(3, ak, pk, 30)).await;

            let processed = ti.get_account(ak, Processed).await.unwrap().unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti.get_account(ak, Confirmed).await.unwrap().unwrap();
            assert_eq!(confirmed.updated_slot, 4);
            assert_eq!(confirmed.account.lamports, 20);

            let finalized = ti.get_account(ak, Finalized).await.unwrap().unwrap();
            assert_eq!(finalized.updated_slot, 3);
            assert_eq!(finalized.account.lamports, 30);
        }

        #[tokio::test]
        async fn becoming_available_after_slot_update() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

            ti.initilize_or_update_account(create_account_data(5, ak, pk, 10)).await;

// Slot = Processed
            ti.process_slot_data(5, Processed).await;

            let processed = ti.get_account(ak, Processed).await.unwrap().unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti.get_account(ak, Confirmed).await.unwrap();
            assert_eq!(confirmed, None);

            let finalized = ti.get_account(ak, Finalized).await.unwrap();
            assert_eq!(finalized, None);

// Slot = Confirmed
            ti.process_slot_data(5, Confirmed).await;

            let processed = ti.get_account(ak, Processed).await.unwrap().unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti.get_account(ak, Confirmed).await.unwrap().unwrap();
            assert_eq!(confirmed.updated_slot, 5);
            assert_eq!(confirmed.account.lamports, 10);

            let finalized = ti.get_account(ak, Finalized).await.unwrap();
            assert_eq!(finalized, None);

// Slot = Finalized
            ti.process_slot_data(5, Finalized).await;

            let processed = ti.get_account(ak, Processed).await.unwrap().unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti.get_account(ak, Confirmed).await.unwrap().unwrap();
            assert_eq!(confirmed.updated_slot, 5);
            assert_eq!(confirmed.account.lamports, 10);

            let finalized = ti.get_account(ak, Finalized).await.unwrap().unwrap();
            assert_eq!(finalized.updated_slot, 5);
            assert_eq!(finalized.account.lamports, 10);
        }
    }

    mod get_program_accounts {
        use std::str::FromStr;

        use solana_rpc_client_api::filter::{Memcmp, RpcFilterType};
        use solana_sdk::pubkey::Pubkey;

        use solana_lite_rpc_core::commitment_utils::Commitment::{Confirmed, Finalized, Processed};

        use crate::account_store_interface::AccountStorageInterface;
        use crate::store::accounts_db::{create_account_data, create_account_data_with_data};
        use crate::store::AccountsDb;

        #[tokio::test]
        async fn different_commitments() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

            ti.process_slot_data(5, Processed).await;
            ti.process_slot_data(4, Confirmed).await;
            ti.process_slot_data(3, Finalized).await;

            ti.initilize_or_update_account(create_account_data(5, ak, pk, 10)).await;
            ti.initilize_or_update_account(create_account_data(4, ak, pk, 20)).await;
            ti.initilize_or_update_account(create_account_data(3, ak, pk, 30)).await;

            let processed = ti.get_program_accounts(pk, None, Processed).await.unwrap().pop().unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti.get_program_accounts(pk, None, Confirmed).await.unwrap().pop().unwrap();
            assert_eq!(confirmed.updated_slot, 4);
            assert_eq!(confirmed.account.lamports, 20);

            let finalized = ti.get_program_accounts(pk, None, Finalized).await.unwrap().pop().unwrap();
            assert_eq!(finalized.updated_slot, 3);
            assert_eq!(finalized.account.lamports, 30);
        }

        #[tokio::test]
        async fn becoming_available_after_slot_update() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

            ti.initilize_or_update_account(create_account_data(5, ak, pk, 10)).await;

// Slot = Processed
            ti.process_slot_data(5, Processed).await;

            let processed = ti.get_program_accounts(pk, None, Processed).await.unwrap().pop().unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti.get_program_accounts(pk, None, Confirmed).await;
            assert_eq!(confirmed, None);

            let finalized = ti.get_program_accounts(pk, None, Finalized).await;
            assert_eq!(finalized, None);

// Slot = Confirmed
            ti.process_slot_data(5, Confirmed).await;

            let processed = ti.get_program_accounts(pk, None, Processed).await.unwrap().pop().unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti.get_program_accounts(pk, None, Confirmed).await.unwrap().pop().unwrap();
            assert_eq!(confirmed.updated_slot, 5);
            assert_eq!(confirmed.account.lamports, 10);

            let finalized = ti.get_program_accounts(pk, None, Finalized).await;
            assert_eq!(finalized, None);

// Slot = Finalized
            ti.process_slot_data(5, Finalized).await;

            let processed = ti.get_program_accounts(pk, None, Processed).await.unwrap().pop().unwrap();
            assert_eq!(processed.updated_slot, 5);
            assert_eq!(processed.account.lamports, 10);

            let confirmed = ti.get_program_accounts(pk, None, Confirmed).await.unwrap().pop().unwrap();
            assert_eq!(confirmed.updated_slot, 5);
            assert_eq!(confirmed.account.lamports, 10);

            let finalized = ti.get_program_accounts(pk, None, Finalized).await.unwrap().pop().unwrap();
            assert_eq!(finalized.updated_slot, 5);
            assert_eq!(finalized.account.lamports, 10);
        }

        #[tokio::test]
        async fn filter_by_data_size() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak1 = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();
            let ak2 = Pubkey::from_str("5VsPdDtqyFw6BmxrTZXKfnTLZy3TgzVA2MA1vZKAfddw").unwrap();

            ti.process_slot_data(5, Processed).await;
            ti.initilize_or_update_account(create_account_data_with_data(5, ak1, pk, Vec::from("abc"))).await;
            ti.initilize_or_update_account(create_account_data_with_data(5, ak2, pk, Vec::from("abcdef"))).await;

            let mut result = ti.get_program_accounts(pk, Some(vec![RpcFilterType::DataSize(3)]), Processed).await.unwrap();
            assert_eq!(result.len(), 1);
            let result = result.pop().unwrap();
            assert_eq!(result.pubkey, ak1);
            assert_eq!(result.updated_slot, 5);
            assert_eq!(result.account.data, Vec::from("abc"));
        }

        #[tokio::test]
        async fn filter_by_mem_cmp() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak1 = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();
            let ak2 = Pubkey::from_str("5VsPdDtqyFw6BmxrTZXKfnTLZy3TgzVA2MA1vZKAfddw").unwrap();

            ti.process_slot_data(5, Processed).await;
            ti.initilize_or_update_account(create_account_data_with_data(5, ak1, pk, Vec::from("abc"))).await;
            ti.initilize_or_update_account(create_account_data_with_data(5, ak2, pk, Vec::from("abcdef"))).await;

            let mut result = ti.get_program_accounts(pk, Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(1, Vec::from("bcdef")))]), Processed).await.unwrap();
            assert_eq!(result.len(), 1);
            let result = result.pop().unwrap();
            assert_eq!(result.pubkey, ak2);
            assert_eq!(result.updated_slot, 5);
            assert_eq!(result.account.data, Vec::from("abcdef"));
        }

        #[tokio::test]
        async fn multiple_filter() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak1 = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();
            let ak2 = Pubkey::from_str("5VsPdDtqyFw6BmxrTZXKfnTLZy3TgzVA2MA1vZKAfddw").unwrap();

            ti.process_slot_data(5, Processed).await;
            ti.initilize_or_update_account(create_account_data_with_data(5, ak1, pk, Vec::from("abc"))).await;
            ti.initilize_or_update_account(create_account_data_with_data(5, ak2, pk, Vec::from("abcdef"))).await;

            let mut result = ti.get_program_accounts(pk, Some(vec![
                RpcFilterType::DataSize(6),
                RpcFilterType::Memcmp(Memcmp::new_raw_bytes(1, Vec::from("bcdef"))),
            ]), Processed).await.unwrap();

            assert_eq!(result.len(), 1);
            let result = result.pop().unwrap();
            assert_eq!(result.pubkey, ak2);
            assert_eq!(result.updated_slot, 5);
            assert_eq!(result.account.data, Vec::from("abcdef"));
        }


        #[tokio::test]
        async fn contradicting_filter() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak1 = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();
            let ak2 = Pubkey::from_str("5VsPdDtqyFw6BmxrTZXKfnTLZy3TgzVA2MA1vZKAfddw").unwrap();

            ti.process_slot_data(5, Processed).await;
            ti.initilize_or_update_account(create_account_data_with_data(5, ak1, pk, Vec::from("abc"))).await;
            ti.initilize_or_update_account(create_account_data_with_data(5, ak2, pk, Vec::from("abcdef"))).await;

            let result = ti.get_program_accounts(pk, Some(vec![
                RpcFilterType::Memcmp(Memcmp::new_raw_bytes(0, Vec::from("a"))),
                RpcFilterType::Memcmp(Memcmp::new_raw_bytes(0, Vec::from("b"))),
            ]), Processed).await;
            assert_eq!(result, None);
        }
    }

    mod process_slot_data {
        use std::str::FromStr;
        use std::sync::atomic::Ordering::Relaxed;

        use solana_sdk::pubkey::Pubkey;

        use solana_lite_rpc_core::commitment_utils::Commitment::{Confirmed, Finalized, Processed};

        use crate::account_store_interface::AccountStorageInterface;
        use crate::store::accounts_db::create_account_data;
        use crate::store::AccountsDb;

        #[tokio::test]
        async fn first_time_invocation() {
            let ti = AccountsDb::new_for_testing();

            ti.process_slot_data(3, Processed).await;
            ti.process_slot_data(2, Confirmed).await;
            ti.process_slot_data(1, Finalized).await;

            assert_eq!(ti.processed_slot.load(Relaxed), 3);
            assert_eq!(ti.confirmed_slot.load(Relaxed), 2);
            assert_eq!(ti.finalised_slot.load(Relaxed), 1);
        }

        #[tokio::test]
        async fn only_updates_processed_slot() {
            let ti = AccountsDb::new_for_testing();

            ti.process_slot_data(1, Processed).await;
            ti.process_slot_data(1, Confirmed).await;
            ti.process_slot_data(1, Finalized).await;

            ti.process_slot_data(2, Processed).await;

            assert_eq!(ti.processed_slot.load(Relaxed), 2);
            assert_eq!(ti.confirmed_slot.load(Relaxed), 1);
            assert_eq!(ti.finalised_slot.load(Relaxed), 1);
        }

        #[tokio::test]
        async fn update_processed_slot_when_confirmed_slot_is_ahead() {
            let ti = AccountsDb::new_for_testing();

            ti.process_slot_data(1, Processed).await;
            ti.process_slot_data(1, Confirmed).await;
            ti.process_slot_data(1, Finalized).await;

            ti.process_slot_data(2, Confirmed).await;

            assert_eq!(ti.processed_slot.load(Relaxed), 2);
            assert_eq!(ti.confirmed_slot.load(Relaxed), 2);
            assert_eq!(ti.finalised_slot.load(Relaxed), 1);
        }

        #[tokio::test]
        async fn update_processed_and_confirmed_slot_when_finalized_slot_is_ahead() {
            let ti = AccountsDb::new_for_testing();

            ti.process_slot_data(1, Processed).await;
            ti.process_slot_data(1, Confirmed).await;
            ti.process_slot_data(1, Finalized).await;

            ti.process_slot_data(2, Finalized).await;

            assert_eq!(ti.processed_slot.load(Relaxed), 2);
            assert_eq!(ti.confirmed_slot.load(Relaxed), 2);
            assert_eq!(ti.finalised_slot.load(Relaxed), 2);
        }

        #[tokio::test]
        async fn returns_updated_account_if_commitment_changes_to_finalized() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

            let result = ti.process_slot_data(3, Processed).await;
            assert_eq!(result.len(), 0);

            ti.initilize_or_update_account(create_account_data(3, ak, pk, 10)).await;

            let result = ti.process_slot_data(3, Confirmed).await;
            assert_eq!(result.len(), 0);

            let result = ti.process_slot_data(3, Finalized).await;
            assert_eq!(result.len(), 1)
        }

        #[tokio::test]
        async fn does_not_return_updated_account_if_different_slot_gets_finalized() {
            let ti = AccountsDb::new_for_testing();

            let pk = Pubkey::from_str("HZGMUF6kdCUK6nuc3TdNR6X5HNdGtg5HmVQ8cV2pRiHE").unwrap();
            let ak = Pubkey::from_str("6rRiMihF7UdJz25t5QvS7PgP9yzfubN7TBRv26ZBVAhE").unwrap();

            ti.initilize_or_update_account(create_account_data(3, ak, pk, 10)).await;
            ti.process_slot_data(3, Finalized).await;

            let result = ti.process_slot_data(4, Finalized).await;
            assert_eq!(result.len(), 0)
        }
    }
}

pub fn create_account_data(
    updated_slot: Slot,
    pubkey: Pubkey,
    program: Pubkey,
    lamports: u64,
) -> AccountData {
    AccountData {
        pubkey,
        account: Arc::new(Account {
            lamports,
            data: Vec::from([]),
            owner: program,
            executable: false,
            rent_epoch: 0,
        }),
        updated_slot,
    }
}


pub fn create_account_data_with_data(
    updated_slot: Slot,
    ak: Pubkey,
    program: Pubkey,
    data: Vec<u8>,
) -> AccountData {
    AccountData {
        pubkey: ak,
        account: Arc::new(Account {
            lamports: 1,
            data,
            owner: program,
            executable: false,
            rent_epoch: 0,
        }),
        updated_slot,
    }
}