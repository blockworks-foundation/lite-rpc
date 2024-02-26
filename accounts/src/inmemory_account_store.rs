use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use dashmap::DashMap;
use itertools::Itertools;
use solana_lite_rpc_core::{commitment_utils::Commitment, structures::account_data::AccountData};
use solana_rpc_client_api::filter::RpcFilterType;
use solana_sdk::{pubkey::Pubkey, slot_history::Slot};
use std::collections::BTreeMap;
use tokio::sync::RwLock;

use crate::account_store_interface::AccountStorageInterface;

#[derive(Clone, Default)]
pub struct AccountDataByCommitment {
    pub pk: Pubkey,
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

    pub fn new(data: AccountData, commitment: Commitment) -> Self {
        let mut processed_accounts = BTreeMap::new();
        processed_accounts.insert(data.updated_slot, data.clone());
        AccountDataByCommitment {
            pk: data.pubkey,
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

    pub fn initialize(data: AccountData) -> Self {
        let mut processed_accounts = BTreeMap::new();
        processed_accounts.insert(data.updated_slot, data.clone());
        AccountDataByCommitment {
            pk: data.pubkey,
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
        if self.processed_accounts.get(&data.updated_slot).is_none() {
            // processed not present for the slot
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
            None
        }
    }
}

pub struct InmemoryAccountStore {
    account_store: Arc<DashMap<Pubkey, AccountDataByCommitment>>,
    confirmed_slots_map: RwLock<BTreeSet<Slot>>,
    owner_map_accounts: Arc<DashMap<Pubkey, HashSet<Pubkey>>>,
}

impl InmemoryAccountStore {
    pub fn new() -> Self {
        Self {
            account_store: Arc::new(DashMap::new()),
            confirmed_slots_map: RwLock::new(BTreeSet::new()),
            owner_map_accounts: Arc::new(DashMap::new()),
        }
    }

    fn add_account_owner(&self, account: Pubkey, owner: Pubkey) {
        match self.owner_map_accounts.entry(owner) {
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
    fn update_owner(
        &self,
        prev_account_data: &AccountData,
        new_account_data: &AccountData,
        commitment: Commitment,
    ) {
        if prev_account_data.pubkey == new_account_data.pubkey
            && prev_account_data.account.owner != new_account_data.account.owner
        {
            if commitment == Commitment::Finalized {
                match self
                    .owner_map_accounts
                    .entry(prev_account_data.account.owner)
                {
                    dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                        occ.get_mut().remove(&prev_account_data.pubkey);
                    }
                    dashmap::mapref::entry::Entry::Vacant(_) => {
                        // do nothing
                    }
                }
            }
            self.add_account_owner(new_account_data.pubkey, new_account_data.account.owner);
        }
    }
}

#[async_trait]
impl AccountStorageInterface for InmemoryAccountStore {
    async fn update_account(&self, account_data: AccountData, commitment: Commitment) -> bool {
        let slot = account_data.updated_slot;
        // check if the blockhash and slot is already confirmed
        let commitment = if commitment == Commitment::Processed {
            let lk = self.confirmed_slots_map.read().await;
            if lk.contains(&slot) {
                Commitment::Confirmed
            } else {
                Commitment::Processed
            }
        } else {
            commitment
        };

        match self.account_store.entry(account_data.pubkey) {
            dashmap::mapref::entry::Entry::Occupied(mut occ) => {
                let prev_account = occ.get().get_account_data(commitment);
                if let Some(prev_account) = prev_account {
                    self.update_owner(&prev_account, &account_data, commitment);
                }
                occ.get_mut().update(account_data, commitment)
            }
            dashmap::mapref::entry::Entry::Vacant(vac) => {
                self.add_account_owner(account_data.pubkey, account_data.account.owner);
                vac.insert(AccountDataByCommitment::new(
                    account_data.clone(),
                    commitment,
                ));
                true
            }
        }
    }

    async fn initilize_account(&self, account_data: AccountData) {
        match self.account_store.contains_key(&account_data.pubkey) {
            true => {
                // account has already been filled by an event
                self.update_account(account_data, Commitment::Finalized)
                    .await;
            }
            false => {
                self.add_account_owner(account_data.pubkey, account_data.account.owner);
                self.account_store.insert(
                    account_data.pubkey,
                    AccountDataByCommitment::initialize(account_data),
                );
            }
        }
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
        program_pubkey: Pubkey,
        account_filters: Option<Vec<RpcFilterType>>,
        commitment: Commitment,
    ) -> Option<Vec<AccountData>> {
        if let Some(program_accounts) = self.owner_map_accounts.get(&program_pubkey) {
            let mut return_vec = vec![];
            for program_account in program_accounts.iter() {
                let account_data = self.get_account(*program_account, commitment).await;
                if let Some(account_data) = account_data {
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
        match commitment {
            Commitment::Confirmed => {
                // insert slot and blockhash that were confirmed
                {
                    let mut lk = self.confirmed_slots_map.write().await;
                    lk.insert(slot);
                }
            }
            Commitment::Finalized => {
                // remove finalized slots form confirmed map
                {
                    let mut lk = self.confirmed_slots_map.write().await;
                    if !lk.remove(&slot) {
                        log::warn!(
                            "following slot {} were not confirmed by account storage",
                            slot
                        );
                    }
                }
            }
            Commitment::Processed => {
                // processed should not use update_slot_data
                log::error!("Processed commitment is not treated by process_slot_data method");
                return vec![];
            }
        }

        let updated_accounts = self
            .account_store
            .iter_mut()
            .filter_map(|mut acc| acc.promote_slot_commitment(slot, commitment))
            .collect_vec();

        // update owners
        updated_accounts
            .iter()
            .for_each(|(account_data, prev_account_data)| {
                if let Some(prev_account_data) = prev_account_data {
                    if prev_account_data.account.owner != account_data.account.owner {
                        self.update_owner(prev_account_data, account_data, commitment);
                    }
                }
            });

        updated_accounts
            .iter()
            .filter_map(|(account_data, prev_account_data)| {
                if let Some(prev_account_data) = prev_account_data {
                    if prev_account_data != account_data {
                        Some(account_data)
                    } else {
                        None
                    }
                } else {
                    Some(account_data)
                }
            })
            .cloned()
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
    use rand::{rngs::ThreadRng, Rng};
    use solana_lite_rpc_core::{
        commitment_utils::Commitment, structures::account_data::AccountData,
    };
    use solana_sdk::{account::Account, pubkey::Pubkey, slot_history::Slot};

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
        AccountData {
            pubkey,
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
        let mut rng = rand::thread_rng();
        let program = Pubkey::new_unique();
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();

        let account_data_0 = create_random_account(&mut rng, 0, pk1, program);
        store.initilize_account(account_data_0.clone()).await;

        let account_data_1 = create_random_account(&mut rng, 0, pk2, program);
        store.initilize_account(account_data_1.clone()).await;

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

        store.process_slot_data(1, Commitment::Confirmed).await;

        assert_eq!(
            store.get_account(pk1, Commitment::Processed).await,
            Some(account_data_5.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Confirmed).await,
            Some(account_data_2.clone())
        );
        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Some(account_data_0.clone())
        );

        store.process_slot_data(2, Commitment::Confirmed).await;

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

        store.process_slot_data(1, Commitment::Finalized).await;

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
            Some(account_data_2.clone())
        );
    }

    #[tokio::test]
    pub async fn test_account_store_if_finalized_clears_old_processed_slots() {
        let store = InmemoryAccountStore::default();

        let program = Pubkey::new_unique();
        let pk1 = Pubkey::new_unique();

        let mut rng = rand::thread_rng();

        store
            .initilize_account(create_random_account(&mut rng, 0, pk1, program))
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
            Some(last_account.clone()),
        );

        // check finalizing previous commitment does not affect
        store.process_slot_data(8, Commitment::Finalized).await;

        assert_eq!(
            store.get_account(pk1, Commitment::Finalized).await,
            Some(last_account),
        );
    }

    #[tokio::test]
    pub async fn test_get_program_account() {
        let store = InmemoryAccountStore::default();

        let prog_1 = Pubkey::new_unique();
        let prog_2 = Pubkey::new_unique();

        let mut rng = rand::thread_rng();

        store
            .update_account(
                create_random_account(&mut rng, 1, Pubkey::new_unique(), prog_1),
                Commitment::Confirmed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, Pubkey::new_unique(), prog_1),
                Commitment::Confirmed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, Pubkey::new_unique(), prog_1),
                Commitment::Confirmed,
            )
            .await;
        store
            .update_account(
                create_random_account(&mut rng, 1, Pubkey::new_unique(), prog_1),
                Commitment::Confirmed,
            )
            .await;

        store
            .update_account(
                create_random_account(&mut rng, 1, Pubkey::new_unique(), prog_2),
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
        let prog_3 = Pubkey::new_unique();

        let account_finalized = create_random_account(&mut rng, 2, pk, prog_3);
        store
            .update_account(account_finalized.clone(), Commitment::Finalized)
            .await;
        store.process_slot_data(2, Commitment::Finalized).await;

        let account_confirmed = create_random_account(&mut rng, 3, pk, prog_3);
        store
            .update_account(account_confirmed.clone(), Commitment::Confirmed)
            .await;

        let prog_4 = Pubkey::new_unique();
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
}
