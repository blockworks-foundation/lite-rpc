use std::{collections::HashSet, sync::Arc};

use crate::{
    account_data_by_commitment::AccountDataByCommitment,
    account_store_interface::{AccountLoadingError, AccountStorageInterface},
    filtered_accounts::FilteredAccounts,
};
use async_trait::async_trait;
use dashmap::DashMap;
use prometheus::{opts, register_int_gauge, IntGauge};
use solana_lite_rpc_core::{
    commitment_utils::Commitment,
    structures::{
        account_data::AccountData,
        account_filter::{AccountFilter, AccountFilters},
    },
};
use solana_rpc_client_api::filter::RpcFilterType;
use solana_sdk::{pubkey::Pubkey, slot_history::Slot};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

lazy_static::lazy_static! {
    static ref ACCOUNT_STORED_IN_MEMORY: IntGauge =
       register_int_gauge!(opts!("literpc_accounts_in_memory", "Account InMemory")).unwrap();

    static ref TOTAL_PROCESSED_ACCOUNTS: IntGauge =
        register_int_gauge!(opts!("literpc_total_processed_accounts_in_memory", "Account processed accounts InMemory")).unwrap();

    static ref SLOT_FOR_LATEST_ACCOUNT_UPDATE: IntGauge =
        register_int_gauge!(opts!("literpc_slot_for_latest_account_update", "Slot of latest account update")).unwrap();
}

struct SlotStatus {
    pub commitment: Commitment,
    pub accounts_updated: HashSet<Pubkey>,
}

pub struct InmemoryAccountStore {
    account_store: Arc<DashMap<Pubkey, AccountDataByCommitment>>,
    accounts_by_owner: Arc<DashMap<Pubkey, HashSet<Pubkey>>>,
    slots_status: Arc<Mutex<BTreeMap<Slot, SlotStatus>>>,
    filtered_accounts: FilteredAccounts,
}

impl InmemoryAccountStore {
    pub fn new(filters: AccountFilters) -> Self {
        let mut filtered_accounts = FilteredAccounts::default();
        filtered_accounts.add_account_filters(&filters);

        Self {
            account_store: Arc::new(DashMap::new()),
            accounts_by_owner: Arc::new(DashMap::new()),
            slots_status: Arc::new(Mutex::new(BTreeMap::new())),
            filtered_accounts,
        }
    }

    fn add_account_owner(&self, account: Pubkey, owner: Pubkey) {
        match self.accounts_by_owner.entry(owner) {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                occ.get_mut().insert(account);
            }
            dashmap::mapref::entry::Entry::Vacant(vc) => {
                let mut set = HashSet::new();
                set.insert(account);
                vc.insert(set);
            }
        }
    }

    // here if the commitment is processed and the account has changed owner from A->B we keep the key for both A and B
    // then we remove the key from A for finalized commitment
    fn update_owner_delete_if_necessary(
        &self,
        prev_account_data: &AccountData,
        new_account_data: &AccountData,
        commitment: Commitment,
    ) {
        assert_eq!(prev_account_data.pubkey, new_account_data.pubkey);
        if prev_account_data.account.owner != new_account_data.account.owner {
            if commitment == Commitment::Finalized {
                match self
                    .accounts_by_owner
                    .entry(prev_account_data.account.owner)
                {
                    dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                        occ.get_mut().remove(&prev_account_data.pubkey);
                    }
                    dashmap::mapref::entry::Entry::Vacant(_) => {
                        // do nothing
                    }
                }

                // account is deleted
                if Self::is_deleted(new_account_data) {
                    self.account_store.remove(&new_account_data.pubkey);
                    return;
                }
            }
            if !Self::is_deleted(new_account_data) && self.satisfies_filters(new_account_data) {
                // update owner if account was not deleted but owner was change and the filter criterias are satisfied
                self.add_account_owner(new_account_data.pubkey, new_account_data.account.owner);
            }
        }
    }

    async fn maybe_update_slot_status(
        &self,
        account_data: &AccountData,
        commitment: Commitment,
    ) -> Commitment {
        let slot = account_data.updated_slot;
        let mut lk = self.slots_status.lock().await;
        let slot_status = match lk.get_mut(&slot) {
            Some(x) => x,
            None => {
                lk.insert(
                    slot,
                    SlotStatus {
                        commitment,
                        accounts_updated: HashSet::new(),
                    },
                );
                lk.get_mut(&slot).unwrap()
            }
        };
        match commitment {
            Commitment::Processed | Commitment::Confirmed => {
                // insert account into slot status
                slot_status.accounts_updated.insert(account_data.pubkey);
                slot_status.commitment
            }
            Commitment::Finalized => commitment,
        }
    }

    pub fn satisfies_filters(&self, account: &AccountData) -> bool {
        self.filtered_accounts.satisfies(account)
    }

    pub fn is_deleted(account: &AccountData) -> bool {
        account.account.lamports == 0
    }

    pub fn add_filter(&mut self, account_filter: &AccountFilter) {
        self.filtered_accounts.add_account_filter(account_filter);
    }
}

#[async_trait]
impl AccountStorageInterface for InmemoryAccountStore {
    async fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool {
        SLOT_FOR_LATEST_ACCOUNT_UPDATE.set(account_data.updated_slot as i64);

        // account is neither deleted, nor tracked, not satifying any filters
        if !Self::is_deleted(&account_data)
            && !self.account_store.contains_key(&account_data.pubkey)
            && !self.satisfies_filters(&account_data)
        {
            return false;
        }

        // check if the blockhash and slot is already confirmed
        let commitment = self
            .maybe_update_slot_status(&account_data, commitment)
            .await;

        match self.account_store.entry(account_data.pubkey) {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                let prev_account = occ.get().get_account_data(commitment);

                // if account has been updated
                if occ.get_mut().update(account_data.clone(), commitment) {
                    if let Some(prev_account) = prev_account {
                        self.update_owner_delete_if_necessary(
                            &prev_account,
                            &account_data,
                            commitment,
                        );
                    }
                    true
                } else {
                    false
                }
            }
            dashmap::mapref::entry::Entry::Vacant(vac) => {
                ACCOUNT_STORED_IN_MEMORY.inc();
                self.add_account_owner(account_data.pubkey, account_data.account.owner);
                vac.insert(AccountDataByCommitment::new(
                    account_data.clone(),
                    commitment,
                ));
                true
            }
        }
    }

    async fn initilize_or_update_account(&self, account_data: AccountData) {
        self.maybe_update_slot_status(&account_data, Commitment::Finalized)
            .await;
        match self.account_store.contains_key(&account_data.pubkey) {
            true => {
                // account has already been filled by an event
                self.update_account(account_data, Commitment::Finalized)
                    .await;
            }
            false => {
                ACCOUNT_STORED_IN_MEMORY.inc();
                self.add_account_owner(account_data.pubkey, account_data.account.owner);
                self.account_store.insert(
                    account_data.pubkey,
                    AccountDataByCommitment::initialize(account_data),
                );
            }
        }
    }

    async fn get_account(
        &self,
        account_pk: Pubkey,
        commitment: Commitment,
    ) -> Result<Option<AccountData>, AccountLoadingError> {
        if let Some(account_by_commitment) = self.account_store.get(&account_pk) {
            Ok(account_by_commitment.get_account_data(commitment).clone())
        } else {
            Ok(None)
        }
    }

    async fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        account_filters: Option<Vec<RpcFilterType>>,
        commitment: Commitment,
    ) -> Option<Vec<AccountData>> {
        if let Some(program_accounts) = self.accounts_by_owner.get(&program_pubkey) {
            let mut return_vec = vec![];
            for program_account in program_accounts.iter() {
                let account_data = self.get_account(*program_account, commitment).await;
                if let Ok(Some(account_data)) = account_data {
                    // recheck program owner and filters
                    if account_data.account.owner.eq(&program_pubkey) {
                        match &account_filters {
                            Some(filters) => {
                                if filters.iter().all(|filter| account_data.allows(filter)) {
                                    return_vec.push(account_data.clone());
                                }
                            }
                            None => {
                                return_vec.push(account_data.clone());
                            }
                        }
                    }
                }
            }
            Some(return_vec)
        } else {
            None
        }
    }

    async fn process_slot_data(&self, slot: Slot, commitment: Commitment) -> Vec<AccountData> {
        let writable_accounts = {
            let mut lk = self.slots_status.lock().await;
            // remove old slot status if finalized
            if commitment == Commitment::Finalized {
                while let Some(entry) = lk.first_entry() {
                    if *entry.key() < slot {
                        entry.remove();
                    } else {
                        break;
                    }
                }
            }
            match lk.get_mut(&slot) {
                Some(status) => {
                    status.commitment = commitment;
                    status.accounts_updated.clone()
                }
                None => {
                    if commitment == Commitment::Confirmed {
                        log::debug!(
                            "slot status not found for {} and commitment {}, confirmed lagging",
                            slot,
                            commitment.into_commitment_level()
                        );
                    } else if commitment == Commitment::Finalized {
                        log::error!("slot status not found for {} and commitment {}, should be normal during startup", slot, commitment.into_commitment_level());
                    }
                    let status = SlotStatus {
                        commitment,
                        accounts_updated: HashSet::new(),
                    };
                    lk.insert(slot, status);
                    HashSet::new()
                }
            }
        };

        let mut updated_accounts = vec![];
        for writable_account in writable_accounts {
            if let Some(mut account) = self.account_store.get_mut(&writable_account) {
                if let Some((account_data, prev_account_data)) =
                    account.promote_slot_commitment(writable_account, slot, commitment)
                {
                    if let Some(prev_account_data) = prev_account_data {
                        // check if owner has changed
                        if prev_account_data.account.owner != account_data.account.owner {
                            self.update_owner_delete_if_necessary(
                                &prev_account_data,
                                &account_data,
                                commitment,
                            );
                        }

                        //check if account data has changed
                        if prev_account_data != account_data {
                            updated_accounts.push(account_data);
                        }
                    } else {
                        // account has been confirmed first time
                        updated_accounts.push(account_data);
                    }
                }
            }
        }

        // update number of processed accounts in memory
        let number_of_processed_accounts_in_memory: i64 = self
            .account_store
            .iter()
            .map(|x| x.processed_accounts.len() as i64)
            .sum();
        TOTAL_PROCESSED_ACCOUNTS.set(number_of_processed_accounts_in_memory);

        updated_accounts
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use itertools::Itertools;
    use rand::{rngs::ThreadRng, Rng};
    use solana_lite_rpc_core::{
        commitment_utils::Commitment,
        structures::{
            account_data::{Account, AccountData},
            account_filter::AccountFilter,
        },
    };
    use solana_sdk::{account::Account as SolanaAccount, pubkey::Pubkey, slot_history::Slot};

    use crate::{
        account_store_interface::AccountStorageInterface,
        inmemory_account_store::InmemoryAccountStore,
    };

    fn create_random_account(
        rng: &mut ThreadRng,
        updated_slot: Slot,
        pubkey: Pubkey,
        program: Pubkey,
    ) -> AccountData {
        let length: usize = rng.gen_range(100..1000);
        let sol_account = SolanaAccount {
            lamports: rng.gen(),
            data: (0..length).map(|_| rng.gen::<u8>()).collect_vec(),
            owner: program,
            executable: false,
            rent_epoch: 0,
        };
        AccountData {
            pubkey,
            account: Arc::new(Account::from_solana_account(
                sol_account,
                solana_lite_rpc_core::structures::account_data::CompressionMethod::None,
            )),
            updated_slot,
            write_version: 0,
        }
    }

    fn create_random_account_with_write_version(
        rng: &mut ThreadRng,
        updated_slot: Slot,
        pubkey: Pubkey,
        program: Pubkey,
        write_version: u64,
    ) -> AccountData {
        let mut acc = create_random_account(rng, updated_slot, pubkey, program);
        acc.write_version = write_version;
        acc
    }

    #[tokio::test]
    pub async fn test_account_store() {
        let program = Pubkey::new_unique();
        let store = InmemoryAccountStore::new(vec![AccountFilter {
            program_id: Some(program.to_string()),
            accounts: vec![],
            filters: None,
        }]);
        let mut rng = rand::thread_rng();
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();

        let account_data_0 = create_random_account(&mut rng, 0, pk1, program);
        store
            .initilize_or_update_account(account_data_0.clone())
            .await;

        let account_data_1 = create_random_account(&mut rng, 0, pk2, program);

        let mut pubkeys = HashSet::new();
        pubkeys.insert(pk1);
        pubkeys.insert(pk2);

        store
            .initilize_or_update_account(account_data_1.clone())
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        assert_eq!(
            store.get_account(pk2, Commitment::Processed).await,
            Ok(Some(account_data_1.clone()))
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Confirmed).await,
            Ok(Some(account_data_1.clone()))
        );
        assert_eq!(
            store.get_account(pk2, Commitment::Finalized).await,
            Ok(Some(account_data_1.clone()))
        );

        let account_data_2 = create_random_account(&mut rng, 1, pk1, program);
        let account_data_3 = create_random_account(&mut rng, 2, pk1, program);
        let account_data_4 = create_random_account(&mut rng, 3, pk1, program);
        let account_data_5 = create_random_account(&mut rng, 4, pk1, program);

        store
            .update_account(account_data_2.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_data_3.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_data_4.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_data_5.clone(), Commitment::Processed)
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        store.process_slot_data(1, Commitment::Confirmed).await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_2.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        store.process_slot_data(2, Commitment::Confirmed).await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        store.process_slot_data(1, Commitment::Finalized).await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_5.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_2.clone()))
        );
    }

    #[tokio::test]
    pub async fn test_account_store_if_finalized_clears_old_processed_slots() {
        let program = Pubkey::new_unique();
        let store = InmemoryAccountStore::new(vec![AccountFilter {
            program_id: Some(program.to_string()),
            accounts: vec![],
            filters: None,
        }]);

        let pk1 = Pubkey::new_unique();

        let mut pubkeys = HashSet::new();
        pubkeys.insert(pk1);

        let mut rng = rand::thread_rng();

        store
            .initilize_or_update_account(create_random_account(&mut rng, 0, pk1, program))
            .await;

        store
            .update_account(
                create_random_account(&mut rng, 1, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 2, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 3, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 4, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 5, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 6, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 7, pk1, program),
                Commitment::Processed,
            )
            .await;

        let account_8 = create_random_account(&mut rng, 8, pk1, program);
        store
            .update_account(account_8.clone(), Commitment::Processed)
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 9, pk1, program),
                Commitment::Processed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 10, pk1, program),
                Commitment::Processed,
            )
            .await;

        let last_account = create_random_account(&mut rng, 11, pk1, program);
        store
            .update_account(last_account.clone(), Commitment::Processed)
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
        store.process_slot_data(11, Commitment::Finalized).await;
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
            Ok(Some(last_account.clone())),
        );

        // check finalizing previous commitment does not affect
        store.process_slot_data(8, Commitment::Finalized).await;

        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(last_account)),
        );
    }

    #[tokio::test]
    pub async fn test_get_program_account() {
        let prog_1 = Pubkey::new_unique();
        let prog_2 = Pubkey::new_unique();
        let prog_3 = Pubkey::new_unique();
        let prog_4 = Pubkey::new_unique();
        let store = InmemoryAccountStore::new(vec![
            AccountFilter {
                program_id: Some(prog_1.to_string()),
                accounts: vec![],
                filters: None,
            },
            AccountFilter {
                program_id: Some(prog_2.to_string()),
                accounts: vec![],
                filters: None,
            },
            AccountFilter {
                program_id: Some(prog_3.to_string()),
                accounts: vec![],
                filters: None,
            },
            AccountFilter {
                program_id: Some(prog_4.to_string()),
                accounts: vec![],
                filters: None,
            },
        ]);

        let mut rng = rand::thread_rng();

        let pks = (0..5).map(|_| Pubkey::new_unique()).collect_vec();

        store
            .update_account(
                create_random_account(&mut rng, 1, pks[0], prog_1),
                Commitment::Confirmed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, pks[1], prog_1),
                Commitment::Confirmed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, pks[2], prog_1),
                Commitment::Confirmed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, pks[3], prog_1),
                Commitment::Confirmed,
            )
            .await;

        store
            .update_account(
                create_random_account(&mut rng, 1, pks[4], prog_2),
                Commitment::Confirmed,
            )
            .await;

        let acc_prgram_1 = store
            .get_program_accounts(prog_1, None, Commitment::Processed)
            .await;
        assert!(acc_prgram_1.is_some());
        assert_eq!(acc_prgram_1.unwrap().len(), 4);
        let acc_prgram_1 = store
            .get_program_accounts(prog_1, None, Commitment::Confirmed)
            .await;
        assert!(acc_prgram_1.is_some());
        assert_eq!(acc_prgram_1.unwrap().len(), 4);
        let acc_prgram_1 = store
            .get_program_accounts(prog_1, None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_1.is_some());
        assert!(acc_prgram_1.unwrap().is_empty());

        let acc_prgram_2 = store
            .get_program_accounts(prog_2, None, Commitment::Processed)
            .await;
        assert!(acc_prgram_2.is_some());
        assert_eq!(acc_prgram_2.unwrap().len(), 1);
        let acc_prgram_2 = store
            .get_program_accounts(prog_2, None, Commitment::Confirmed)
            .await;
        assert!(acc_prgram_2.is_some());
        assert_eq!(acc_prgram_2.unwrap().len(), 1);
        let acc_prgram_2 = store
            .get_program_accounts(prog_2, None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_2.is_some());
        assert!(acc_prgram_2.unwrap().is_empty());

        let acc_prgram_3 = store
            .get_program_accounts(Pubkey::new_unique(), None, Commitment::Processed)
            .await;
        assert!(acc_prgram_3.is_none());
        let acc_prgram_3 = store
            .get_program_accounts(Pubkey::new_unique(), None, Commitment::Confirmed)
            .await;
        assert!(acc_prgram_3.is_none());
        let acc_prgram_3 = store
            .get_program_accounts(Pubkey::new_unique(), None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_3.is_none());

        store.process_slot_data(1, Commitment::Finalized).await;

        let acc_prgram_1 = store
            .get_program_accounts(prog_1, None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_1.is_some());
        assert_eq!(acc_prgram_1.unwrap().len(), 4);
        let acc_prgram_2 = store
            .get_program_accounts(prog_2, None, Commitment::Finalized)
            .await;
        assert!(acc_prgram_2.is_some());
        assert_eq!(acc_prgram_2.unwrap().len(), 1);

        let pk = Pubkey::new_unique();

        let account_finalized = create_random_account(&mut rng, 2, pk, prog_3);
        store
            .update_account(account_finalized.clone(), Commitment::Finalized)
            .await;
        store.process_slot_data(2, Commitment::Finalized).await;

        let account_confirmed = create_random_account(&mut rng, 3, pk, prog_3);
        store
            .update_account(account_confirmed.clone(), Commitment::Confirmed)
            .await;

        let account_processed = create_random_account(&mut rng, 4, pk, prog_4);
        store
            .update_account(account_processed.clone(), Commitment::Processed)
            .await;

        let f = store
            .get_program_accounts(prog_3, None, Commitment::Finalized)
            .await;

        let c = store
            .get_program_accounts(prog_3, None, Commitment::Confirmed)
            .await;

        let p_3 = store
            .get_program_accounts(prog_3, None, Commitment::Processed)
            .await;

        let p_4 = store
            .get_program_accounts(prog_4, None, Commitment::Processed)
            .await;

        assert_eq!(c, Some(vec![account_confirmed.clone()]));
        assert_eq!(p_3, Some(vec![]));
        assert_eq!(p_4, Some(vec![account_processed.clone()]));

        assert_eq!(f, Some(vec![account_finalized.clone()]));

        store.process_slot_data(3, Commitment::Finalized).await;
        store.process_slot_data(4, Commitment::Confirmed).await;

        let f = store
            .get_program_accounts(prog_3, None, Commitment::Finalized)
            .await;

        let p_3 = store
            .get_program_accounts(prog_3, None, Commitment::Confirmed)
            .await;

        let p_4 = store
            .get_program_accounts(prog_4, None, Commitment::Confirmed)
            .await;

        assert_eq!(f, Some(vec![account_confirmed.clone()]));
        assert_eq!(p_3, Some(vec![]));
        assert_eq!(p_4, Some(vec![account_processed.clone()]));

        store.process_slot_data(4, Commitment::Finalized).await;
        let p_3 = store
            .get_program_accounts(prog_3, None, Commitment::Finalized)
            .await;

        let p_4 = store
            .get_program_accounts(prog_4, None, Commitment::Finalized)
            .await;

        assert_eq!(p_3, Some(vec![]));
        assert_eq!(p_4, Some(vec![account_processed.clone()]));
    }

    #[tokio::test]
    pub async fn writing_old_account_state() {
        let program = Pubkey::new_unique();
        let store = InmemoryAccountStore::new(vec![AccountFilter {
            program_id: Some(program.to_string()),
            accounts: vec![],
            filters: None,
        }]);
        let mut rng = rand::thread_rng();
        let pk1 = Pubkey::new_unique();

        // setting random account as finalized at slot 0
        let account_data_0 = create_random_account(&mut rng, 0, pk1, program);
        store
            .initilize_or_update_account(account_data_0.clone())
            .await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_0.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_0.clone()))
        );

        // updating state for processed at slot 3
        let account_data_slot_3 = create_random_account(&mut rng, 3, pk1, program);
        store
            .update_account(account_data_slot_3.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_0.clone()))
        );

        // updating state for processed at slot 2
        let account_data_slot_2 = create_random_account(&mut rng, 2, pk1, program);
        store
            .update_account(account_data_slot_2.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_0.clone()))
        );

        // confirming slot 2
        let updates = store.process_slot_data(2, Commitment::Confirmed).await;
        assert_eq!(updates, vec![account_data_slot_2.clone()]);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_slot_2.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        // confirming random state at slot 1 / does not do anything as slot 2 has already been confrimed
        let account_data_slot_1 = create_random_account(&mut rng, 1, pk1, program);
        store
            .update_account(account_data_slot_1.clone(), Commitment::Confirmed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_slot_2.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_0.clone()))
        );

        // making slot 3 finalized
        let updates = store.process_slot_data(3, Commitment::Finalized).await;
        assert_eq!(updates, vec![account_data_slot_3.clone()]);
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_slot_3.clone()))
        );

        // making slot 2 finalized
        let updates = store.process_slot_data(2, Commitment::Finalized).await;
        assert!(updates.is_empty());
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_slot_3.clone()))
        );

        // useless old updates
        let account_data_slot_1_2 = create_random_account(&mut rng, 1, pk1, program);
        let account_data_slot_2_2 = create_random_account(&mut rng, 2, pk1, program);
        store
            .update_account(account_data_slot_1_2.clone(), Commitment::Processed)
            .await;
        store
            .update_account(account_data_slot_2_2.clone(), Commitment::Confirmed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_slot_3.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_slot_3.clone()))
        );
    }

    #[tokio::test]
    pub async fn account_states_with_different_write_version() {
        let program = Pubkey::new_unique();
        let store = InmemoryAccountStore::new(vec![AccountFilter {
            program_id: Some(program.to_string()),
            accounts: vec![],
            filters: None,
        }]);
        let mut rng = rand::thread_rng();
        let pk1 = Pubkey::new_unique();

        // setting random account as finalized at slot 0
        let account_data_10 =
            create_random_account_with_write_version(&mut rng, 1, pk1, program, 10);
        store
            .update_account(account_data_10.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_10.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(None)
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(None)
        );

        // with higher write version process account is updated
        let account_data_11 =
            create_random_account_with_write_version(&mut rng, 1, pk1, program, 11);
        store
            .update_account(account_data_11.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_11.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(None)
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(None)
        );

        // with lower write version process account is not updated
        let account_data_9 = create_random_account_with_write_version(&mut rng, 1, pk1, program, 9);
        store
            .update_account(account_data_9.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_11.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(None)
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(None)
        );

        // with finalized commitment all the last account version is taken into account
        store.process_slot_data(1, Commitment::Finalized).await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_11.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_11.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_11.clone()))
        );

        // if the account for slot is updated after with higher account write version both processed and finalized slots are updated
        let account_data_12 =
            create_random_account_with_write_version(&mut rng, 1, pk1, program, 12);
        store
            .update_account(account_data_12.clone(), Commitment::Processed)
            .await;
        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Ok(Some(account_data_12.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Ok(Some(account_data_12.clone()))
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Ok(Some(account_data_12.clone()))
        );
    }
}
