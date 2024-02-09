use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use dashmap::DashMap;
use itertools::Itertools;
use solana_lite_rpc_core::{commitment_utils::Commitment, structures::account_data::AccountData};
use solana_rpc_client_api::filter::RpcFilterType;
use solana_sdk::{hash::Hash, pubkey::Pubkey, slot_history::Slot};
use std::collections::BTreeMap;
use tokio::sync::RwLock;

use crate::account_store_interface::AccountStorageInterface;

#[derive(Clone, Default)]
pub struct AccountDataByCommitment {
    pub processed_accounts: BTreeMap<(Slot, Hash), AccountData>,
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

    pub fn new(block_hash: Hash, data: AccountData, commitment: Commitment) -> Self {
        let mut processed_accounts = BTreeMap::new();
        processed_accounts.insert((data.updated_slot, block_hash), data.clone());
        AccountDataByCommitment {
            processed_accounts,
            confirmed_account: if commitment == Commitment::Confirmed {
                Some(data)
            } else {
                None
            },
            finalized_account: None,
        }
    }

    pub fn initialize(data: AccountData) -> Self {
        AccountDataByCommitment {
            processed_accounts: BTreeMap::new(),
            confirmed_account: Some(data.clone()),
            finalized_account: Some(data),
        }
    }

    pub fn update(&mut self, hash: Hash, data: AccountData, commitment: Commitment) {
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

        if self
            .processed_accounts
            .get(&(data.updated_slot, hash))
            .is_none()
        {
            // processed not present for the slot
            self.processed_accounts
                .insert((data.updated_slot, hash), data.clone());
        }
        match commitment {
            Commitment::Confirmed => {
                if update_confirmed {
                    self.confirmed_account = Some(data);
                }
            }
            Commitment::Finalized => {
                if update_confirmed {
                    self.confirmed_account = Some(data.clone());
                }
                if update_finalized {
                    self.finalized_account = Some(data);
                }
            }
            Commitment::Processed => {
                // processed already treated
            }
        }
    }
    // this method will promote processed account to confirmed account to finalized account
    // returns promoted account
    pub fn promote_slot_commitment(
        &mut self,
        hash: Hash,
        slot: Slot,
        commitment: Commitment,
    ) -> Option<AccountData> {
        let key = (slot, hash);
        if let Some(account_data) = self.processed_accounts.get(&key).cloned() {
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
                        self.confirmed_account = Some(account_data.clone());
                        Some(account_data)
                    } else {
                        None
                    }
                }
                Commitment::Finalized => {
                    // slot finalized remove data from processed
                    while self
                        .processed_accounts
                        .first_key_value()
                        .map(|((s, _), _)| *s)
                        .unwrap_or(u64::MAX)
                        <= slot
                    {
                        self.processed_accounts.pop_first();
                    }

                    // processed map should not be empty
                    if self.processed_accounts.is_empty() {
                        log::error!(
                            "Processed map should not be empty filling it with finalized data"
                        );
                        self.processed_accounts.insert(key, account_data.clone());
                    }

                    if self
                        .finalized_account
                        .as_ref()
                        .map(|acc| acc.updated_slot)
                        .unwrap_or_default()
                        < slot
                    {
                        self.finalized_account = Some(account_data.clone());
                        Some(account_data)
                    } else {
                        None
                    }
                }
            }
        } else {
            // remove processed slot data
            while self
                .processed_accounts
                .first_key_value()
                .map(|((s, _), _)| *s)
                .unwrap_or(u64::MAX)
                <= slot
            {
                log::error!("Should not be the case, we should have seen this block in processed atleast once to reach here. Something seems broken");
                self.processed_accounts.pop_first();
            }
            None
        }
    }
}

pub struct InmemoryAccountStore {
    account_store: Arc<DashMap<Pubkey, AccountDataByCommitment>>,
    confirmed_slots_map: RwLock<BTreeSet<(Slot, Hash)>>,
}

impl InmemoryAccountStore {
    pub fn new() -> Self {
        Self {
            account_store: Arc::new(DashMap::new()),
            confirmed_slots_map: RwLock::new(BTreeSet::new()),
        }
    }
}

#[async_trait]
impl AccountStorageInterface for InmemoryAccountStore {
    async fn update_processed_account(
        &self,
        account_pk: Pubkey,
        account_data: AccountData,
        block_hash: Hash,
    ) {
        let slot = account_data.updated_slot;
        // check if the blockhash and slot is already confirmed
        let commitment = {
            let lk = self.confirmed_slots_map.read().await;
            if lk.contains(&(slot, block_hash)) {
                Commitment::Confirmed
            } else {
                Commitment::Processed
            }
        };

        if let Some(mut account_by_commitment) = self.account_store.get_mut(&account_pk) {
            account_by_commitment.update(block_hash, account_data, commitment);
        } else {
            self.account_store.insert(
                account_pk,
                AccountDataByCommitment::new(block_hash, account_data.clone(), commitment),
            );
        }
    }

    async fn initilize_account(&self, account_pk: Pubkey, account_data: AccountData) {
        self.account_store.insert(
            account_pk,
            AccountDataByCommitment::initialize(account_data),
        );
    }

    async fn get_account(&self, account_pk: Pubkey, commitment: Commitment) -> Option<AccountData> {
        if let Some(account_by_commitment) = self.account_store.get(&account_pk) {
            account_by_commitment.get_account_data(commitment).clone()
        } else {
            None
        }
    }

    async fn get_program_accounts(
        &self,
        program_pubkey: &Pubkey,
        account_filter: &Option<RpcFilterType>,
        commitment: Commitment,
    ) -> Vec<AccountData> {
        self.account_store
            .iter()
            .filter_map(|acc| {
                let acc_data = acc.get_account_data(commitment);
                match acc_data {
                    Some(acc_data) => {
                        if acc_data.account.owner.eq(program_pubkey) {
                            match account_filter {
                                Some(filter) => {
                                    if acc_data.allows(filter) {
                                        Some(acc_data.clone())
                                    } else {
                                        None
                                    }
                                }
                                None => Some(acc_data.clone()),
                            }
                        } else {
                            None
                        }
                    }
                    None => None,
                }
            })
            .collect_vec()
    }

    async fn process_slot_data(
        &self,
        slot: Slot,
        block_hash: Hash,
        commitment: Commitment,
    ) -> Vec<AccountData> {
        match commitment {
            Commitment::Confirmed => {
                // insert slot and blockhash that were confirmed
                {
                    let mut lk = self.confirmed_slots_map.write().await;
                    lk.insert((slot, block_hash));
                }
            }
            Commitment::Finalized => {
                // remove finalized slots form confirmed map
                {
                    let mut lk = self.confirmed_slots_map.write().await;
                    if !lk.remove(&(slot, block_hash)) {
                        log::warn!("following slot {} and blockhash {} were not confirmed by account storage", slot, block_hash.to_string());
                    }
                }
            }
            Commitment::Processed => {
                // processed should not use update_slot_data
                log::error!("Invalid commitment");
                return vec![];
            }
        }

        self.account_store
            .iter_mut()
            .filter_map(|mut acc| acc.promote_slot_commitment(block_hash, slot, commitment))
            .collect_vec()
    }
}

impl Default for InmemoryAccountStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::{Rng, SeedableRng};
    use solana_lite_rpc_core::{
        commitment_utils::Commitment, structures::account_data::AccountData,
    };
    use solana_sdk::{account::Account, hash::Hash, pubkey::Pubkey, slot_history::Slot};

    use crate::{
        account_store_interface::AccountStorageInterface,
        inmemory_account_store::InmemoryAccountStore,
    };

    fn create_random_account(seed: u64, updated_slot: Slot, program: Pubkey) -> AccountData {
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
        let length: usize = rng.gen_range(100..1000);
        AccountData {
            account: Account {
                lamports: rng.gen(),
                data: (0..length).map(|_| rng.gen::<u8>()).collect_vec(),
                owner: program,
                executable: false,
                rent_epoch: 0,
            },
            updated_slot,
        }
    }

    #[tokio::test]
    pub async fn test_account_store() {
        let store = InmemoryAccountStore::default();

        let program = Pubkey::new_unique();
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();

        let account_data_0 = create_random_account(0, 0, program);
        store.initilize_account(pk1, account_data_0.clone()).await;

        let account_data_1 = create_random_account(1, 0, program);
        store.initilize_account(pk2, account_data_1.clone()).await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Some(account_data_0.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Some(account_data_0.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Some(account_data_0.clone())
        );

        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await,
            Some(account_data_1.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await,
            Some(account_data_1.clone())
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await,
            Some(account_data_1.clone())
        );

        let account_data_2 = create_random_account(2, 1, program);
        let account_data_2_bis = create_random_account(200, 1, program);
        let account_data_3 = create_random_account(3, 2, program);
        let account_data_3_bis = create_random_account(300, 2, program);
        let account_data_4 = create_random_account(4, 3, program);
        let account_data_5 = create_random_account(5, 4, program);

        let block_hash_1 = Hash::new_unique();
        let block_hash_1_bis = Hash::new_unique();
        let block_hash_2 = Hash::new_unique();
        let block_hash_2_bis = Hash::new_unique();
        let block_hash_3 = Hash::new_unique();
        let block_hash_4 = Hash::new_unique();

        store
            .update_processed_account(pk1, account_data_2.clone(), block_hash_1)
            .await;
        store
            .update_processed_account(pk1, account_data_3.clone(), block_hash_2)
            .await;
        store
            .update_processed_account(pk1, account_data_2_bis.clone(), block_hash_1_bis)
            .await;
        store
            .update_processed_account(pk1, account_data_3_bis.clone(), block_hash_2_bis)
            .await;
        store
            .update_processed_account(pk1, account_data_4.clone(), block_hash_3)
            .await;
        store
            .update_processed_account(pk1, account_data_5.clone(), block_hash_4)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Some(account_data_5.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Some(account_data_0.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Some(account_data_0.clone())
        );

        store
            .process_slot_data(1, block_hash_1_bis, Commitment::Confirmed)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Some(account_data_5.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Some(account_data_2_bis.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Some(account_data_0.clone())
        );

        store
            .process_slot_data(2, block_hash_2, Commitment::Confirmed)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Some(account_data_5.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Some(account_data_3.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Some(account_data_0.clone())
        );

        store
            .process_slot_data(1, block_hash_1_bis, Commitment::Finalized)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Some(account_data_5.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Some(account_data_3.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Some(account_data_2_bis.clone())
        );
    }

    #[tokio::test]
    pub async fn test_account_store_if_finalized_clears_old_processed_slots() {
        let store = InmemoryAccountStore::default();

        let program = Pubkey::new_unique();
        let pk1 = Pubkey::new_unique();

        store
            .initilize_account(pk1, create_random_account(0, 0, program))
            .await;

        store
            .update_processed_account(
                pk1,
                create_random_account(0, 1, program),
                Hash::new_unique(),
            )
            .await;
        store
            .update_processed_account(
                pk1,
                create_random_account(1, 1, program),
                Hash::new_unique(),
            )
            .await;
        store
            .update_processed_account(
                pk1,
                create_random_account(0, 2, program),
                Hash::new_unique(),
            )
            .await;
        store
            .update_processed_account(
                pk1,
                create_random_account(0, 3, program),
                Hash::new_unique(),
            )
            .await;
        store
            .update_processed_account(
                pk1,
                create_random_account(0, 4, program),
                Hash::new_unique(),
            )
            .await;
        store
            .update_processed_account(
                pk1,
                create_random_account(0, 5, program),
                Hash::new_unique(),
            )
            .await;
        store
            .update_processed_account(
                pk1,
                create_random_account(0, 6, program),
                Hash::new_unique(),
            )
            .await;
        store
            .update_processed_account(
                pk1,
                create_random_account(0, 7, program),
                Hash::new_unique(),
            )
            .await;

        let hash_8 = Hash::new_unique();
        let account_8 = create_random_account(8, 8, program);
        store
            .update_processed_account(pk1, account_8.clone(), hash_8)
            .await;
        store
            .update_processed_account(
                pk1,
                create_random_account(0, 9, program),
                Hash::new_unique(),
            )
            .await;
        store
            .update_processed_account(
                pk1,
                create_random_account(10, 10, program),
                Hash::new_unique(),
            )
            .await;
        let last_hash = Hash::new_unique();
        let last_account = create_random_account(11, 11, program);
        store
            .update_processed_account(pk1, last_account.clone(), last_hash)
            .await;

        assert_eq!(
            store
                .account_store
                .get(&pk1)
                .unwrap()
                .processed_accounts
                .len(),
            12
        );
        store
            .process_slot_data(11, last_hash, Commitment::Finalized)
            .await;
        assert_eq!(
            store
                .account_store
                .get(&pk1)
                .unwrap()
                .processed_accounts
                .len(),
            1
        );

        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Some(last_account.clone()),
        );

        // check finalizing previous commitment does not affect
        store
            .process_slot_data(8, hash_8, Commitment::Finalized)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Some(last_account),
        );
    }
}
